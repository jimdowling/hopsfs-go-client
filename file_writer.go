package hdfs

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/v2/internal/transfer"
	"google.golang.org/protobuf/proto"
)

const MaxSmallFileSize = 1024 * 64

const maxWriteChunkSize = 64 << 10

var ErrReplicating = errors.New("replication in progress")

// IsErrReplicating returns true if the passed error is an os.PathError wrapping
// ErrReplicating.
func IsErrReplicating(err error) bool {
	pe, ok := err.(*os.PathError)
	return ok && pe.Err == ErrReplicating
}

// A FileWriter represents a writer for an open file in HDFS. It implements
// Writer and Closer, and can only be used for writes. For reads, see
// FileReader and Client.Open.
type FileWriter struct {
	client *Client
	name   string
	// leaseHeld is true from a successful create or append until complete succeeds, while the namenode
	// connection must renew this client's lease.
	leaseHeld   bool
	replication int
	blockSize   int64
	fileId      *uint64

	blockWriter     *transfer.BlockWriter
	deadline        time.Time
	storeInDB       bool
	smallFileBuffer []byte
	pos             uint64
	writeOffset     uint64
	lastBlock       *hdfs.ExtendedBlockProto
	encryptedBuffer []byte
	lastError       error
	closed          bool
	closing         bool
	// newGS is the bumped generation stamp returned by
	// updateBlockForPipeline during Append(). It is the GS the DN
	// finalized at, so the `complete` RPC must report this value as the
	// lastBlock's GS, not the pre-bump GS stored on blockWriter.Block.B.
	newGS uint64

	// Key and IV for transparent encryption support.
	enc *transparentEncryptionInfo
}

// Create opens a new file in HDFS with the default replication, block size,
// and permissions (0644), and returns an io.WriteCloser for writing
// to it. Because of the way that HDFS writes are buffered and acknowledged
// asynchronously, it is very important that Close is called after all data has
// been written.
func (c *Client) Create(name string) (*FileWriter, error) {
	defaults, err := c.fetchDefaults()
	if err != nil {
		return nil, err
	}

	replication := int(defaults.GetReplication())
	blockSize := int64(defaults.GetBlockSize())
	return c.CreateFile(name, replication, blockSize, 0644, false, false)
}

// CreateFile opens a new file in HDFS with the given replication, block size,
// and permissions, and returns an io.WriteCloser for writing to it. Because of
// the way that HDFS writes are buffered and acknowledged asynchronously, it is
// very important that Close is called after all data has been written.
func (c *Client) CreateFile(name string, replication int, blockSize int64, perm os.FileMode, overwrite bool, createParent bool) (*FileWriter, error) {
	return c.createFileWithGroup(name, replication, blockSize, perm, overwrite, createParent, "")
}

// CreateFileWithGroup opens a new file in HDFS with the given replication, block size,
// permissions, and group name, and returns an io.WriteCloser for writing to it.
// If groupname is empty, the file inherits the group from its parent directory.
// Because of the way that HDFS writes are buffered and acknowledged asynchronously,
// it is very important that Close is called after all data has been written.
func (c *Client) CreateFileWithGroup(name string, replication int, blockSize int64, perm os.FileMode, overwrite bool, createParent bool, groupname string) (*FileWriter, error) {
	return c.createFileWithGroup(name, replication, blockSize, perm, overwrite, createParent, groupname)
}

func (c *Client) createFileWithGroup(name string, replication int, blockSize int64, perm os.FileMode, overwrite bool, createParent bool, groupname string) (*FileWriter, error) {
	if replication <= 0 || replication > 32767 || blockSize <= 0 {
		return nil, &os.PathError{Op: "create", Path: name, Err: os.ErrInvalid}
	}
	createFlag := proto.Uint32(1)
	if overwrite {
		createFlag = proto.Uint32(3) // 0x01 for Create and 0x10 for overwrite
	}

	createReq := &hdfs.CreateRequestProto{
		Src:                   proto.String(name),
		Masked:                &hdfs.FsPermissionProto{Perm: proto.Uint32(uint32(perm))},
		ClientName:            proto.String(c.namenode.ClientName),
		CreateFlag:            createFlag,
		CreateParent:          proto.Bool(createParent),
		Replication:           proto.Uint32(uint32(replication)),
		BlockSize:             proto.Uint64(uint64(blockSize)),
		CryptoProtocolVersion: []hdfs.CryptoProtocolVersionProto{hdfs.CryptoProtocolVersionProto_ENCRYPTION_ZONES},
	}

	// Set groupname if provided (non-empty)
	if groupname != "" {
		createReq.Groupname = proto.String(groupname)
	}

	createResp := &hdfs.CreateResponseProto{}

	err := c.namenode.Execute("create", createReq, createResp)
	if err != nil {
		return nil, &os.PathError{Op: "create", Path: name, Err: interpretCreateException(err)}
	}
	// Count the granted lease before KMS lookups so renewal also covers slow key retrieval.
	c.namenode.AcquireLease()

	storedInDB := false
	if createResp.GetFs().GetStoragePolicy() == uint32(14) {
		storedInDB = true
	}

	var enc *transparentEncryptionInfo
	if createResp.GetFs().GetFileEncryptionInfo() != nil {
		enc, err = c.kmsGetKey(createResp.GetFs().GetFileEncryptionInfo())
		if err != nil {
			c.namenode.ReleaseLease()
			_ = c.Remove(name)
			return nil, &os.PathError{Op: "create", Path: name, Err: err}
		}
	}

	var fileID *uint64
	if status := createResp.GetFs(); status != nil {
		fileID = status.FileId
	}
	f := &FileWriter{
		client:          c,
		name:            name,
		replication:     replication,
		blockSize:       blockSize,
		fileId:          fileID,
		storeInDB:       storedInDB,
		smallFileBuffer: []byte{},
		pos:             0,
		enc:             enc,
		leaseHeld:       true,
	}
	return f, nil
}

// Append opens an existing file in HDFS and returns an io.WriteCloser for
// writing to it. Because of the way that HDFS writes are buffered and
// acknowledged asynchronously, it is very important that Close is called after
// all data has been written.
func (c *Client) Append(name string) (_ *FileWriter, err error) {
	var f *FileWriter
	leased := false
	// A writer that fails after the namenode granted the lease is not returned, so nothing would release it.
	defer func() {
		if err == nil {
			return
		}
		if f != nil {
			f.releaseLease()
		} else if leased {
			c.namenode.ReleaseLease()
		}
	}()

	appendReq := &hdfs.AppendRequestProto{
		Src:        proto.String(name),
		ClientName: proto.String(c.namenode.ClientName),
	}
	appendResp := &hdfs.AppendResponseProto{}

	initDelay := time.Duration(100)
	for i := 0; i < 9; i++ {
		err = c.namenode.Execute("append", appendReq, appendResp)
		// Retry on transient append-time errors:
		//   - NotReplicatedYetException: NN-side wraps this in RetriableException
		//     when the last block is COMMITTED (waiting for IBR to drive it to
		//     COMPLETE).
		//   - "is not sufficiently replicated yet": NN's appendFileInternal
		//     throws plain IOException when the last block is COMPLETE but
		//     liveReplicas < minReplication. This is also transient — it
		//     resolves once DN block reports land. The Java client never
		//     reproducibly hits this because its DataStreamer waits for ack
		//     on prior writes, but the Go client's tight loop in
		//     TestFileAppendRepeatedly races it.
		if err != nil &&
			(strings.Contains(err.Error(), "NotReplicatedYetException") ||
				strings.Contains(err.Error(), "is not sufficiently replicated yet")) {
			if i == 8 {
				break
			}
			time.Sleep(initDelay * time.Millisecond)
			initDelay *= 2
		} else if err != nil {
			return nil, &os.PathError{Op: "append", Path: name, Err: interpretException(err)}
		} else {
			break
		}
	}
	// Retry budget exhausted while the last error was still retryable —
	// surface it instead of falling through with a nil appendResp.Stat,
	// which would NPE at the FileWriter construction below.
	if err != nil {
		return nil, &os.PathError{Op: "append", Path: name, Err: interpretException(err)}
	}
	// Granted with the append; counted before the KMS request for the same reason as in create.
	c.namenode.AcquireLease()
	leased = true
	if appendResp.GetStat() == nil {
		info, statErr := c.getFileInfo(name)
		if statErr != nil {
			return nil, &os.PathError{Op: "append", Path: name, Err: interpretException(statErr)}
		}
		appendResp.Stat = info.Sys().(*FileStatus)
	}

	var enc *transparentEncryptionInfo
	if appendResp.GetStat().GetFileEncryptionInfo() != nil {
		enc, err = c.kmsGetKey(appendResp.GetStat().GetFileEncryptionInfo())
		if err != nil {
			return nil, &os.PathError{Op: "append", Path: name, Err: err}
		}
	}

	f = &FileWriter{
		client:          c,
		name:            name,
		replication:     int(appendResp.Stat.GetBlockReplication()),
		blockSize:       int64(appendResp.Stat.GetBlocksize()),
		fileId:          appendResp.Stat.FileId,
		storeInDB:       false,
		smallFileBuffer: []byte{},
		pos:             appendResp.GetStat().GetLength(),
		writeOffset:     appendResp.GetStat().GetLength(),
		enc:             enc,
		leaseHeld:       true,
	}

	// This returns nil if there are no blocks (it's an empty file) or if the
	// last block is full (so we have to start a fresh block).
	block := appendResp.GetBlock()
	if block == nil {
		return f, nil
	}

	//handling appending to phantom block
	if len(appendResp.GetBlock().Data) > 0 {
		f.storeInDB = true
		f.writeOffset = 0
		f.smallFileBuffer = appendResp.GetBlock().Data
		return f, nil
	}

	// Bump the generation stamp for this append, mirroring the Java
	// DataStreamer's setupPipelineForAppendOrRecovery flow. The NN's
	// prepareFileForAppend returns the un-bumped block; without this
	// second RPC the DN sees latestGenerationStamp == block.GS and
	// CloudFsDatasetImpl.appendInternal adds the current GS into
	// ProvidedReplicaBeingWritten's oldGS list, scheduling a
	// self-delete of the just-uploaded cloud object (HOPSFS-345
	// partial-chunk-append self-delete).
	oldExtBlock := block.GetB()
	updReq := &hdfs.UpdateBlockForPipelineRequestProto{
		Block:      oldExtBlock,
		ClientName: proto.String(c.namenode.ClientName),
	}
	updResp := &hdfs.UpdateBlockForPipelineResponseProto{}
	err = c.namenode.Execute("updateBlockForPipeline", updReq, updResp)
	if err != nil {
		return nil, &os.PathError{Op: "append", Path: name, Err: interpretException(err)}
	}
	newLocatedBlock := updResp.GetBlock()
	newGS := newLocatedBlock.GetB().GetGenerationStamp()

	// Confirm the new GS + pipeline targets on the NN. Java's
	// DataStreamer calls updatePipeline only after writeBlock succeeds
	// (so it can retry with new nodes if the pipeline failed). The Go
	// client has no pipeline recovery, so we commit to the freshly
	// returned pipeline upfront and rely on the eventual `complete`
	// RPC to surface any node-level failure.
	//
	// The locations come from the original `append` response, NOT from
	// the updateBlockForPipeline response: FSNamesystem.updateBlockForPipeline
	// returns a LocatedBlock with empty DatanodeInfo[] (see
	// FSNamesystem.java:6591 — `new LocatedBlock(block, new DatanodeInfo[0])`).
	// Passing those empty locations into updatePipeline would NPE inside
	// BlockInfoContiguousUnderConstruction.setExpectedLocations.
	newExtBlock := &hdfs.ExtendedBlockProto{
		PoolId:          oldExtBlock.PoolId,
		BlockId:         oldExtBlock.BlockId,
		GenerationStamp: proto.Uint64(newGS),
		NumBytes:        oldExtBlock.NumBytes,
		// Preserve cloudBucket: HopsFS's FSNamesystem.commitOrCompleteLastBlock
		// (line 4312) and appendFileInternal (line 2386) branch on
		// commitBlock.isProvidedBlock(), which is true iff cloudBucket !=
		// NON_EXISTENT_BUCKET_NAME. Dropping this field makes the NN treat
		// the next append's last block as a regular HDFS block and wait
		// indefinitely for liveReplicas >= minReplication.
		CloudBucket: oldExtBlock.CloudBucket,
	}
	newNodes := make([]*hdfs.DatanodeIDProto, 0, len(block.GetLocs()))
	for _, info := range block.GetLocs() {
		newNodes = append(newNodes, info.GetId())
	}
	updPipReq := &hdfs.UpdatePipelineRequestProto{
		ClientName: proto.String(c.namenode.ClientName),
		OldBlock:   oldExtBlock,
		NewBlock:   newExtBlock,
		NewNodes:   newNodes,
		StorageIDs: block.GetStorageIDs(),
	}
	updPipResp := &hdfs.UpdatePipelineResponseProto{}
	err = c.namenode.Execute("updatePipeline", updPipReq, updPipResp)
	if err != nil {
		return nil, &os.PathError{Op: "append", Path: name, Err: interpretException(err)}
	}
	f.newGS = newGS

	dialFunc, err := f.client.wrapDatanodeDial(
		f.client.options.DatanodeDialFunc,
		block.GetBlockToken())
	if err != nil {
		return nil, err
	}

	f.blockWriter = &transfer.BlockWriter{
		ClientName:          f.client.namenode.ClientName,
		Block:               block,
		BlockSize:           f.blockSize,
		Offset:              int64(block.B.GetNumBytes()),
		Append:              true,
		NewGS:               newGS,
		UseDatanodeHostname: f.client.options.UseDatanodeHostname,
		DialFunc:            dialFunc,
	}

	err = f.blockWriter.SetDeadline(f.deadline)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// CreateEmptyFile creates a empty file at the given name, with the
// permissions 0644.
func (c *Client) CreateEmptyFile(name string) error {
	f, err := c.Create(name)
	if err != nil {
		return err
	}

	err = f.Close()
	if err != nil {
		f.Abort()
	}
	return err
}

// SetDeadline sets the deadline for future Write, Flush, and Close calls. A
// zero value for t means those calls will not time out.
//
// Note that because of buffering, Write calls that do not result in a blocking
// network call may still succeed after the deadline.
func (f *FileWriter) SetDeadline(t time.Time) error {
	f.deadline = t
	if f.blockWriter != nil {
		return f.blockWriter.SetDeadline(t)
	}

	// Return the error at connection time.
	return nil
}

// Write implements io.Writer for writing to a file in HDFS. Internally, it
// writes data to an internal buffer first, and then later out to HDFS. Because
// of this, it is important that Close is called after all data has been
// written.
func (f *FileWriter) Write(b []byte) (int, error) {
	if f.lastError != nil {
		return 0, f.lastError
	}
	if f.closed || f.closing {
		return 0, io.ErrClosedPipe
	}
	if len(b) == 0 {
		return 0, nil
	}
	if f.storeInDB {
		if len(b) <= MaxSmallFileSize-len(f.smallFileBuffer) {
			f.smallFileBuffer = append(f.smallFileBuffer, b...)
			f.pos += uint64(len(b))
			return len(b), nil
		}
		if err := f.promoteSmallFile(); err != nil {
			return 0, f.fail(err)
		}
	}
	n, err := f.writeInternal(b)
	f.pos += uint64(n)
	if err != nil {
		err = f.fail(err)
	}
	return n, err
}

func (f *FileWriter) promoteSmallFile() error {
	if len(f.smallFileBuffer) > 0 {
		if _, err := f.writeInternal(f.smallFileBuffer); err != nil {
			return err
		}
	}
	f.smallFileBuffer = nil
	f.storeInDB = false
	return nil
}

func (f *FileWriter) writeInternal(b []byte) (int, error) {
	if f.blockWriter == nil {
		err := f.startNewBlock()
		if err != nil {
			return 0, err
		}
	}

	off := 0
	for off < len(b) {
		if f.blockWriter.Offset >= f.blockWriter.BlockSize {
			if err := f.startNewBlock(); err != nil {
				return off, err
			}
		}
		count := len(b) - off
		if count > maxWriteChunkSize {
			count = maxWriteChunkSize
		}
		if remaining := f.blockWriter.BlockSize - f.blockWriter.Offset; int64(count) > remaining {
			count = int(remaining)
		}
		if count <= 0 {
			return off, errors.New("invalid block capacity")
		}
		part := b[off : off+count]
		if f.enc != nil {
			if f.enc.stream == nil {
				var err error
				f.enc.stream, err = aesCreateCTRStream(int64(f.writeOffset), f.enc)
				if err != nil {
					return off, err
				}
			}
			if cap(f.encryptedBuffer) < count {
				f.encryptedBuffer = make([]byte, count)
			}
			output := f.encryptedBuffer[:count]
			f.enc.stream.XORKeyStream(output, part)
			part = output
		}
		n, err := f.blockWriter.Write(part)
		if f.enc != nil && n != count {
			f.enc.stream = nil
		}
		off += n
		f.writeOffset += uint64(n)
		if err == transfer.ErrEndOfBlock {
			err = f.startNewBlock()
		}
		if err != nil {
			return off, err
		}
		if n == 0 {
			return off, io.ErrNoProgress
		}
	}

	return off, nil
}

// Flush flushes any buffered data out to the datanodes. Even immediately after
// a call to Flush, it is still necessary to call Close once all data has been
// written.
func (f *FileWriter) Flush() error {
	if f.lastError != nil {
		return f.lastError
	}
	if f.closed || f.closing {
		return io.ErrClosedPipe
	}
	if f.storeInDB && len(f.smallFileBuffer) > 0 {
		if err := f.promoteSmallFile(); err != nil {
			return f.fail(err)
		}
	}

	if f.blockWriter != nil {
		err := f.blockWriter.Flush()
		if err != nil {
			return f.fail(err)
		}
	}

	return nil
}

// Close closes the file, writing any remaining data out to disk and waiting
// for acknowledgements from the datanodes. It is important that Close is called
// after all data has been written.
//
// If the datanodes have acknowledged all writes but not yet to the namenode,
// it can return ErrReplicating (wrapped in an os.PathError). This indicates
// that all data has been written, but the lease is still open for the file.
// Retry Close until completion, or call Abort to stop renewing the lease and
// allow server-side recovery. Closing the Client also stops renewal.
func (f *FileWriter) Close() error {
	f.closing = true
	err := f.closeInt()
	if err != nil {
		// if the close failed due to the DB throwing
		// OutOfExtents Exception then we retry the close
		// operation after writing the data to disk

		if f.storeInDB && len(f.smallFileBuffer) > 0 &&
			strings.Contains(err.Error(), "OutOfDBExtentsException") {

			if err := f.promoteSmallFile(); err != nil {
				return f.fail(err)
			}

			err = f.closeInt()
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return nil
}

func (f *FileWriter) closeInt() error {
	if f.lastError != nil {
		return f.fail(f.lastError)
	}
	if f.closed {
		return nil
	}

	if !f.storeInDB && f.blockWriter != nil {
		f.lastBlock = f.currentBlock()
		if err := f.closeBlock(); err != nil {
			return f.fail(err)
		}
	}

	completeReq := &hdfs.CompleteRequestProto{
		Src:        proto.String(f.name),
		ClientName: proto.String(f.client.namenode.ClientName),
		Last:       f.lastBlock,
	}

	if f.storeInDB {
		completeReq.Data = f.smallFileBuffer
	}

	completeResp := &hdfs.CompleteResponseProto{}

	sleep := time.Duration(100)
	for i := 0; i < 10; i++ {
		err := f.execute("complete", completeReq, completeResp)
		if err != nil {
			return &os.PathError{Op: "create", Path: f.name, Err: interpretException(err)}
		}

		if completeResp.Result == nil {
			return errors.New("missing complete response result")
		}
		closed := completeResp.GetResult()

		if !closed { //retry after sleep
			if i == 9 {
				break
			}
			if err := f.waitRetry(sleep * time.Millisecond); err != nil {
				return err
			}
			sleep *= 2
			continue
		} else {
			f.closed = true
			f.smallFileBuffer = nil
			f.encryptedBuffer = nil
			f.releaseLease()
			return nil
		}
	}

	return &os.PathError{Op: "create", Path: f.name, Err: ErrReplicating}
}

func (f *FileWriter) currentBlock() *hdfs.ExtendedBlockProto {
	block := proto.Clone(f.blockWriter.Block.GetB()).(*hdfs.ExtendedBlockProto)
	if f.newGS != 0 {
		block.GenerationStamp = proto.Uint64(f.newGS)
	}
	return block
}

// Abort discards buffered data and stops renewal without completing or deleting the file.
// Data already acknowledged by DataNodes remains subject to NameNode lease recovery.
func (f *FileWriter) Abort() error {
	if f.closed && f.lastError == nil {
		return nil
	}
	f.fail(io.ErrClosedPipe)
	return nil
}

func (f *FileWriter) execute(method string, request, response proto.Message) error {
	ctx := context.Background()
	if !f.deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, f.deadline)
		defer cancel()
	}
	return f.client.namenode.ExecuteContext(ctx, method, request, response)
}

func (f *FileWriter) waitRetry(delay time.Duration) error {
	ctx := context.Background()
	if !f.deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, f.deadline)
		defer cancel()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *FileWriter) fail(err error) error {
	if f.lastError == nil {
		f.lastError = err
	}
	if f.blockWriter != nil {
		f.blockWriter.Abort()
	}
	f.smallFileBuffer = nil
	f.encryptedBuffer = nil
	f.closed = true
	f.releaseLease()
	return f.lastError
}

func (f *FileWriter) releaseLease() {
	if f.leaseHeld {
		f.leaseHeld = false
		f.client.namenode.ReleaseLease()
	}
}

func (f *FileWriter) startNewBlock() error {
	var previous *hdfs.ExtendedBlockProto
	if f.blockWriter != nil {
		previous = f.currentBlock()

		// TODO: We don't actually need to wait for previous blocks to ack before
		// continuing.
		err := f.closeBlock()
		if err != nil {
			return err
		}
	}

	addBlockResp, err := f.addBlockWithRetry(previous)
	if err != nil {
		return &os.PathError{Op: "create", Path: f.name, Err: interpretException(err)}
	}

	block := addBlockResp.GetBlock()
	dialFunc, err := f.client.wrapDatanodeDial(
		f.client.options.DatanodeDialFunc, block.GetBlockToken())
	if err != nil {
		return err
	}

	f.newGS = 0
	f.lastBlock = nil
	f.blockWriter = &transfer.BlockWriter{
		ClientName:          f.client.namenode.ClientName,
		Block:               block,
		BlockSize:           f.blockSize,
		UseDatanodeHostname: f.client.options.UseDatanodeHostname,
		DialFunc:            dialFunc,
	}

	return f.blockWriter.SetDeadline(f.deadline)
}

func (f *FileWriter) addBlockWithRetry(previous *hdfs.ExtendedBlockProto) (*hdfs.AddBlockResponseProto, error) {
	addBlockReq := &hdfs.AddBlockRequestProto{
		Src:        proto.String(f.name),
		ClientName: proto.String(f.client.namenode.ClientName),
		Previous:   previous,
	}

	addBlockResp := &hdfs.AddBlockResponseProto{}
	initDelay := time.Duration(400)
	var err error = nil

	for i := 0; i < 8; i++ {
		err = f.execute("addBlock", addBlockReq, addBlockResp)
		if err != nil && strings.Contains(err.Error(), "NotReplicatedYetException") {
			if i == 7 {
				break
			}
			if waitErr := f.waitRetry(initDelay * time.Millisecond); waitErr != nil {
				return nil, waitErr
			}
			initDelay *= 2
		} else {
			break
		}
	}
	return addBlockResp, err
}

func (f *FileWriter) closeBlock() error {
	err := f.blockWriter.Close()
	if err != nil {
		return err
	}

	f.blockWriter = nil
	return nil
}

func (f *FileWriter) GetPos() uint64 {
	return f.pos
}
