package userclient

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Profile struct {
	ID string `json:"id"`
	FirstName string `json:"first_name"`
	LastName string `json:"last_name"`
}
type FullResponse struct {
	Profile *Profile `json:"profile,omitempty"`
}
type Client struct {
	baseURL string
	httpClient *http.Client
	hmacKey []byte
}
func New(baseURL, hmacKey string) *Client {
	return &Client{baseURL: baseURL, httpClient: &http.Client{Timeout: 3 * time.Second}, hmacKey: []byte(hmacKey)}
}
func (c *Client) signRequest(r *http.Request) {
	if len(c.hmacKey) == 0 { return }
	uid := r.Header.Get("X-User-Id")
	email := r.Header.Get("X-User-Email")
	role := r.Header.Get("X-User-Role")
	payload := uid + "|" + email + "|" + role
	mac := hmac.New(sha256.New, c.hmacKey)
	mac.Write([]byte(payload))
	r.Header.Set("X-Signature", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
}
func (c *Client) GetName(ctx context.Context, userID string) string {
	url := fmt.Sprintf("%s/internal/users/%s", c.baseURL, userID)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Set("X-User-Id", "00000000-0000-0000-0000-000000000000")
	req.Header.Set("X-User-Email", "system@diploma")
	req.Header.Set("X-User-Role", "admin")
	c.signRequest(req)
	resp, err := c.httpClient.Do(req)
	if err != nil { return "" }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { return "" }
	var full FullResponse
	json.NewDecoder(resp.Body).Decode(&full)
	if full.Profile == nil { return "" }
	if full.Profile.FirstName != "" {
		return full.Profile.FirstName + " " + full.Profile.LastName
	}
	return ""
}
