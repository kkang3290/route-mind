// Package anthropic implements the Anthropic Messages provider.
package anthropic

import "github.com/kkang3290/route-mind/pkg/llm/providers/transport"

func New(options transport.Options) (*transport.Client, error) {
	if options.BaseURL == "" {
		options.BaseURL = "https://api.anthropic.com/v1"
	}
	return transport.New("anthropic", options)
}
