package header_auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
)

// HeaderSigner signs and verifies internal headers using HMAC-SHA256.
// It prevents backend services from accepting forged X-User-Id, X-User-Email,
// and X-User-Role headers from services that can reach them directly.
type HeaderSigner struct {
	key []byte
}

// NewHeaderSigner creates a new HeaderSigner with the given HMAC key.
// The key should be a shared secret known only to the API gateway and
// internal backend services.
func NewHeaderSigner(key string) *HeaderSigner {
	return &HeaderSigner{key: []byte(key)}
}

// SignHeaders computes an HMAC-SHA256 signature over the internal user
// headers (X-User-Id, X-User-Email, X-User-Role) and sets the X-Signature
// header on the request. Backend services use VerifyHeaders to validate
// that the headers were set by a trusted source (the API gateway).
func (s *HeaderSigner) SignHeaders(r *http.Request) {
	userID := r.Header.Get("X-User-Id")
	userEmail := r.Header.Get("X-User-Email")
	userRole := r.Header.Get("X-User-Role")

	payload := strings.Join([]string{userID, userEmail, userRole}, "|")
	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte(payload))
	sig := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	r.Header.Set("X-Signature", sig)
}

// VerifyHeaders checks that the X-Signature header matches the HMAC-SHA256
// of the internal user headers. Returns true if the signature is valid.
// If X-User-Id is not present (e.g., public endpoint), verification is
// skipped and returns true.
func (s *HeaderSigner) VerifyHeaders(r *http.Request) bool {
	userID := r.Header.Get("X-User-Id")
	userEmail := r.Header.Get("X-User-Email")
	userRole := r.Header.Get("X-User-Role")

	// If no internal headers are present, this is a public endpoint
	// that does not require signature verification.
	if userID == "" && userEmail == "" && userRole == "" {
		return true
	}

	expectedSig := r.Header.Get("X-Signature")
	if expectedSig == "" {
		return false
	}

	payload := strings.Join([]string{userID, userEmail, userRole}, "|")
	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte(payload))
	computedSig := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	return hmac.Equal([]byte(computedSig), []byte(expectedSig))
}

// VerifyMiddleware returns an HTTP middleware that verifies the X-Signature
// header on incoming requests. If internal user headers are present but the
// signature is missing or invalid, the middleware responds with 401 and
// stops the request chain.
func (s *HeaderSigner) VerifyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.VerifyHeaders(r) {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "недействительная подпись внутреннего заголовка",
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}
