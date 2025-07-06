package errorreport

import (
	"context"
	"io"
	"log/slog"
)

type ErrorReport struct {
}

func New() *ErrorReport {
	return &ErrorReport{}
}

func (e *ErrorReport) AcceptErrors(errCh <-chan error, target io.Writer, cancel context.CancelFunc) {
	for {
		error, ok := <-errCh
		if !ok {
			slog.Info("error channel is closed: closing down error reporter")
			return
		}

		toWrite := error.Error() + "\n"
		if _, err := target.Write([]byte(toWrite)); err != nil {
			slog.Error("error writing to file", slog.Any("err", err))
			cancel()
			slog.Info("closing down error reporter")
			return
		}
	}
}
