package matrix

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/turt2live/matrix-media-repo/common/config"
)

// Based in part on https://github.com/matrix-org/gomatrix/blob/072b39f7fa6b40257b4eead8c958d71985c28bdd/client.go#L180-L243
func doRequest(method string, urlStr string, body interface{}, result interface{}, accessToken string, ipAddr string) (error) {
	var bodyBytes []byte
	if body != nil {
		jsonStr, err := json.Marshal(body)
		if err != nil {
			return err
		}

		bodyBytes = jsonStr
	}

	req, err := http.NewRequest(method, urlStr, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return err
	}

	req.Header.Set("User-Agent", "matrix-media-repo")
	req.Header.Set("Content-Type", "application/json")
	if accessToken != "" {
		req.Header.Set("Authorization", "Bearer "+accessToken)
	}
	if ipAddr != "" {
		req.Header.Set("X-Forwarded-For", ipAddr)
		req.Header.Set("X-Real-IP", ipAddr)
	}

	client := &http.Client{
		Timeout: time.Duration(config.Get().TimeoutSeconds.ClientServer) * time.Second,
	}
	res, err := client.Do(req)
	if res != nil {
		defer res.Body.Close()
	}
	if err != nil {
		return err
	}

	contents, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		mtxErr := &errorResponse{}
		err = json.Unmarshal(contents, mtxErr)
		if err == nil && mtxErr.ErrorCode != "" {
			return mtxErr
		}
		return errors.New("failed to perform request: " + string(contents))
	}

	if result != nil {
		err = json.Unmarshal(contents, &result)
		if err != nil {
			return err
		}
	}

	return nil
}

func makeUrl(parts ... string) string {
	res := ""
	for _, p := range parts {
		if p[len(p)-1:] == "/" {
			res += p[:len(p)-1]
		} else {
			res += p
		}
	}
	return res
}
