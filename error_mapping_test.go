package hdfs

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

type protocolError struct{ exception, message string }

func (e protocolError) Error() string     { return e.message }
func (e protocolError) Method() string    { return "test" }
func (e protocolError) Desc() string      { return "ERROR" }
func (e protocolError) Exception() string { return e.exception }
func (e protocolError) Message() string   { return e.message }

func TestWrappedHopsFSExceptionsRetainMeaning(t *testing.T) {
	require.ErrorIs(t, interpretException(protocolError{javaIOException, fileNotFoundException + ": missing"}), os.ErrNotExist)
	require.ErrorIs(t, interpretCreateException(protocolError{javaIOException, alreadyBeingCreatedException + ": busy"}), os.ErrExist)
	require.ErrorIs(t, interpretException(protocolError{javaIOException, permissionDeniedException + ": denied"}), os.ErrPermission)
}
