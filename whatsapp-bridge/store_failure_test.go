package main

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"google.golang.org/protobuf/proto"
)

func TestHandleMessage_StoreFailureSkipsMediaDownloadButForwardsWebhook(t *testing.T) {
	srv, webhookCh := captureWebhook(t)
	t.Setenv("WEBHOOK_URL", srv.URL)

	client := newTestClient(&mockLIDStore{})
	ms := newTestMessageStore(t)
	// Make StoreMessage fail deterministically while StoreChat still works.
	if _, err := ms.db.Exec("DROP TABLE messages"); err != nil {
		t.Fatalf("drop messages: %v", err)
	}

	msg := buildImageMessage(phonePN, phonePN, false, "")
	msg.Message.ImageMessage.URL = proto.String("https://example.invalid/image")
	msg.Message.ImageMessage.MediaKey = []byte("test-media-key")

	originalDownload := downloadMediaForMessage
	downloads := make(chan struct{}, 4)
	downloadMediaForMessage = func(_ *whatsmeow.Client, _ *MessageStore, _ string, _ string) (bool, string, string, string, error) {
		downloads <- struct{}{}
		return false, "", "", "", nil
	}
	t.Cleanup(func() { downloadMediaForMessage = originalDownload })

	handleMessage(client, ms, msg, testLogger())

	// downloadMedia looks the row up by message ID; with no row it can only
	// fail, so it must not be attempted (synchronously or in the background).
	select {
	case <-downloads:
		t.Fatal("media download attempted although the message row was not stored")
	case <-time.After(200 * time.Millisecond):
	}

	// The text/notification must still reach the webhook consumer.
	select {
	case payload := <-webhookCh:
		if payload.MessageID != "test-img-001" {
			t.Errorf("expected messageId=test-img-001, got %q", payload.MessageID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("webhook must still be sent when the local store fails")
	}
}
