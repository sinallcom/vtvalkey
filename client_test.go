package vtvalkey

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

func TestWriteCommand(t *testing.T) {
	var buf bytes.Buffer
	if err := writeCommand(&buf, "SET", "key", "value"); err != nil {
		t.Fatal(err)
	}
	want := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	if got := buf.String(); got != want {
		t.Errorf("writeCommand:\n got %q\nwant %q", got, want)
	}
}

func TestReadReplySimpleString(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("+OK\r\n"))
	rp, err := readReply(r)
	if err != nil {
		t.Fatal(err)
	}
	if rp.kind != replyString || rp.str != "OK" {
		t.Errorf("expected string OK, got %+v", rp)
	}
}

func TestReadReplyError(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("-ERR unknown command\r\n"))
	rp, err := readReply(r)
	if err != nil {
		t.Fatal(err)
	}
	if rp.kind != replyError {
		t.Errorf("expected error reply, got %+v", rp)
	}
	if err := rp.toError(); err == nil {
		t.Error("expected non-nil error")
	}
}

func TestReadReplyInteger(t *testing.T) {
	r := bufio.NewReader(strings.NewReader(":42\r\n"))
	rp, err := readReply(r)
	if err != nil {
		t.Fatal(err)
	}
	n, err := rp.toInt64()
	if err != nil {
		t.Fatal(err)
	}
	if n != 42 {
		t.Errorf("expected 42, got %d", n)
	}
}

func TestReadReplyBulkString(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("$5\r\nhello\r\n"))
	rp, err := readReply(r)
	if err != nil {
		t.Fatal(err)
	}
	s, err := rp.toString()
	if err != nil {
		t.Fatal(err)
	}
	if s != "hello" {
		t.Errorf("expected hello, got %q", s)
	}
}

func TestReadReplyNilBulk(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("$-1\r\n"))
	rp, err := readReply(r)
	if err != nil {
		t.Fatal(err)
	}
	_, err = rp.toString()
	if err != ErrNil {
		t.Errorf("expected ErrNil, got %v", err)
	}
}

func TestReadReplyArray(t *testing.T) {
	input := "*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n"
	r := bufio.NewReader(strings.NewReader(input))
	rp, err := readReply(r)
	if err != nil {
		t.Fatal(err)
	}
	sl, err := rp.toStrSlice()
	if err != nil {
		t.Fatal(err)
	}
	if len(sl) != 3 || sl[0] != "foo" || sl[1] != "bar" || sl[2] != "baz" {
		t.Errorf("unexpected slice: %v", sl)
	}
}

func TestReadReplyStrMap(t *testing.T) {
	input := "*4\r\n$4\r\nname\r\n$5\r\nalice\r\n$3\r\nage\r\n$2\r\n30\r\n"
	r := bufio.NewReader(strings.NewReader(input))
	rp, err := readReply(r)
	if err != nil {
		t.Fatal(err)
	}
	m, err := rp.toStrMap()
	if err != nil {
		t.Fatal(err)
	}
	if m["name"] != "alice" || m["age"] != "30" {
		t.Errorf("unexpected map: %v", m)
	}
}

func TestReadReplyScanEntry(t *testing.T) {
	// SCAN returns: *2\r\n $<cursor>\r\n *<elements>...
	input := "*2\r\n$1\r\n0\r\n*2\r\n$6\r\nvt:ip1\r\n$6\r\nvt:ip2\r\n"
	r := bufio.NewReader(strings.NewReader(input))
	rp, err := readReply(r)
	if err != nil {
		t.Fatal(err)
	}
	if rp.kind != replyArray || len(rp.items) != 2 {
		t.Fatalf("expected 2-element array, got %+v", rp)
	}
	// Verify cursor
	if rp.items[0].str != "0" {
		t.Errorf("expected cursor 0, got %q", rp.items[0].str)
	}
	// Verify elements
	elements, err := rp.items[1].toStrSlice()
	if err != nil {
		t.Fatal(err)
	}
	if len(elements) != 2 || elements[0] != "vt:ip1" {
		t.Errorf("unexpected elements: %v", elements)
	}
}
