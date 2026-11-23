package llm

import (
	"encoding/json"
	"errors"
)

func jsonUnmarshalObject(data []byte, out *map[string]any) error {
	if err := json.Unmarshal(data, out); err != nil {
		return err
	}
	if *out == nil {
		return errors.New("expected JSON object")
	}
	return nil
}
