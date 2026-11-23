// Package openai implements the OpenAI Chat Completions provider.
package openai

import "github.com/kkang3290/route-mind/pkg/llm/providers/transport"

func New(options transport.Options) (*transport.Client, error) {
	if options.BaseURL == "" {
		options.BaseURL = "https://api.openai.com/v1"
	}
	return transport.New("openai", options)
}
