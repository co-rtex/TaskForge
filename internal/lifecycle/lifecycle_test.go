package lifecycle

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateErrorCode_AcceptsStableTokensOnly(t *testing.T) {
	for _, code := range []string{
		"handler_error", "attempt_timeout", "http.502", "db-timeout", "a", "x9",
		strings.Repeat("a", MaxErrorCodeBytes),
	} {
		require.NoErrorf(t, ValidateErrorCode(code), "%q should be accepted", code)
	}

	for name, code := range map[string]string{
		"empty":            "",
		"uppercase":        "HandlerError",
		"leading dash":     "-bad",
		"space":            "handler error",
		"newline":          "handler\nerror",
		"slash":            "handler/error",
		"too long":         strings.Repeat("a", MaxErrorCodeBytes+1),
		"non ascii":        "错误",
		"leading punctuat": ".leading",
	} {
		require.Errorf(t, ValidateErrorCode(code), "%s (%q) should be rejected", name, code)
	}
}

func TestValidateErrorMessage_RejectsControlCharactersAndOversize(t *testing.T) {
	require.NoError(t, ValidateErrorMessage(""), "a code alone is a complete answer")
	require.NoError(t, ValidateErrorMessage("upstream returned 502"))
	require.NoError(t, ValidateErrorMessage(strings.Repeat("m", MaxErrorMessageBytes)))

	require.Error(t, ValidateErrorMessage(strings.Repeat("m", MaxErrorMessageBytes+1)))
	require.Error(t, ValidateErrorMessage("first line\nsecond line"))
	require.Error(t, ValidateErrorMessage("carriage\rreturn"))
	require.Error(t, ValidateErrorMessage("null\x00byte"))
	require.Error(t, ValidateErrorMessage("bell\x07"))
}

func TestSafeMessage_CollapsesAndBoundsWithoutInventingContent(t *testing.T) {
	require.Equal(t, "", SafeMessage("   "))
	require.Equal(t, "upstream returned 502", SafeMessage("  upstream returned 502  "))
	require.Equal(t, "line one line two", SafeMessage("line one\nline two"))
	require.Equal(t, "tabbed value", SafeMessage("tabbed\tvalue"))
	require.Equal(t, "nullstripped", SafeMessage("null\x00stripped"))

	long := SafeMessage(strings.Repeat("m", MaxErrorMessageBytes*2))
	require.Len(t, long, MaxErrorMessageBytes)
	require.NoError(t, ValidateErrorMessage(long))
}

// TestSafeMessage_TruncatesOnARuneBoundary keeps the stored value valid UTF-8:
// cutting a multi-byte rune in half would put an invalid string into a TEXT
// column and into every JSON response that reads it back.
func TestSafeMessage_TruncatesOnARuneBoundary(t *testing.T) {
	// 3 bytes per rune, so a byte-oriented cut at 512 lands mid-rune.
	message := SafeMessage(strings.Repeat("é", MaxErrorMessageBytes))
	require.LessOrEqual(t, len(message), MaxErrorMessageBytes)
	require.True(t, isValidUTF8(message), "truncation split a rune")
	require.NoError(t, ValidateErrorMessage(message))
}

func isValidUTF8(s string) bool {
	for _, r := range s {
		if r == '�' {
			return false
		}
	}
	return true
}

func TestDLQReason_SetIsClosed(t *testing.T) {
	require.True(t, ReasonPermanentFailure.Valid())
	require.True(t, ReasonAttemptsExhausted.Valid())
	require.False(t, DLQReason("EXHAUSTED").Valid())
	require.False(t, DLQReason("").Valid())
}
