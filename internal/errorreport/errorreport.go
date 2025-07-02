package errorreport

import (
	"context"
	"io"
	"log/slog"
)

type ErrorReport struct {
	errCh  <-chan error
	writer io.Writer
}

func New(errCh <-chan error, writer io.Writer) *ErrorReport {
	return &ErrorReport{
		errCh:  errCh,
		writer: writer,
	}
}

func (e *ErrorReport) AcceptErrors(cancel context.CancelFunc) {
	for {
		error, ok := <-e.errCh
		if !ok {
			slog.Info("error channel is closed: closing down error reporter")
			return
		}

		toWrite := error.Error() + "\n"
		if _, err := e.writer.Write([]byte(toWrite)); err != nil {
			slog.Error("error writing to file", slog.Any("err", err))
			cancel()
			slog.Info("closing down error reporter")
			return
		}
	}
}
