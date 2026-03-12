package vtvalkey

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// RESP wire protocol encoder/decoder (RESP2).

// writeCommand encodes a Redis command as a RESP array of bulk strings.
func writeCommand(w io.Writer, args ...string) error {
	// *<count>\r\n
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, a := range args {
		// $<len>\r\n<data>\r\n
		if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(a), a); err != nil {
			return err
		}
	}
	return nil
}

// reply types returned by readReply.
type replyKind int

const (
	replyString  replyKind = iota // +OK\r\n
	replyError                    // -ERR ...\r\n
	replyInteger                  // :123\r\n
	replyBulk                     // $<len>\r\n<data>\r\n  or $-1\r\n (nil)
	replyArray                    // *<count>\r\n ...
)

type reply struct {
	kind   replyKind
	str    string  // for string/error/bulk
	num    int64   // for integer
	items  []reply // for array
	isNil  bool    // bulk nil ($-1)
}

func readReply(r *bufio.Reader) (reply, error) {
	line, err := readLine(r)
	if err != nil {
		return reply{}, err
	}
	if len(line) == 0 {
		return reply{}, fmt.Errorf("vtvalkey: empty reply line")
	}

	switch line[0] {
	case '+':
		return reply{kind: replyString, str: line[1:]}, nil

	case '-':
		return reply{kind: replyError, str: line[1:]}, nil

	case ':':
		n, err := strconv.ParseInt(line[1:], 10, 64)
		if err != nil {
			return reply{}, fmt.Errorf("vtvalkey: bad integer reply: %w", err)
		}
		return reply{kind: replyInteger, num: n}, nil

	case '$':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return reply{}, fmt.Errorf("vtvalkey: bad bulk length: %w", err)
		}
		if n < 0 {
			return reply{kind: replyBulk, isNil: true}, nil
		}
		buf := make([]byte, n+2) // data + \r\n
		if _, err := io.ReadFull(r, buf); err != nil {
			return reply{}, fmt.Errorf("vtvalkey: read bulk: %w", err)
		}
		return reply{kind: replyBulk, str: string(buf[:n])}, nil

	case '*':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return reply{}, fmt.Errorf("vtvalkey: bad array count: %w", err)
		}
		if n < 0 {
			return reply{kind: replyArray, isNil: true}, nil
		}
		items := make([]reply, n)
		for i := range items {
			items[i], err = readReply(r)
			if err != nil {
				return reply{}, err
			}
		}
		return reply{kind: replyArray, items: items}, nil

	default:
		return reply{}, fmt.Errorf("vtvalkey: unknown reply type %q", line[0])
	}
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) >= 2 && line[len(line)-2] == '\r' {
		return line[:len(line)-2], nil
	}
	return line[:len(line)-1], nil
}

// toError converts a reply to an error if applicable.
func (rp reply) toError() error {
	if rp.kind == replyError {
		return fmt.Errorf("vtvalkey: %s", rp.str)
	}
	return nil
}

func (rp reply) toString() (string, error) {
	if err := rp.toError(); err != nil {
		return "", err
	}
	if rp.isNil {
		return "", ErrNil
	}
	return rp.str, nil
}

func (rp reply) toInt64() (int64, error) {
	if err := rp.toError(); err != nil {
		return 0, err
	}
	if rp.kind == replyInteger {
		return rp.num, nil
	}
	// Some commands return integers as bulk strings
	if rp.kind == replyBulk {
		return strconv.ParseInt(rp.str, 10, 64)
	}
	return 0, fmt.Errorf("vtvalkey: expected integer, got %d", rp.kind)
}

func (rp reply) toStrSlice() ([]string, error) {
	if err := rp.toError(); err != nil {
		return nil, err
	}
	if rp.isNil {
		return nil, nil
	}
	if rp.kind != replyArray {
		return nil, fmt.Errorf("vtvalkey: expected array, got %d", rp.kind)
	}
	out := make([]string, len(rp.items))
	for i, it := range rp.items {
		if it.isNil {
			continue
		}
		out[i] = it.str
	}
	return out, nil
}

func (rp reply) toStrMap() (map[string]string, error) {
	if err := rp.toError(); err != nil {
		return nil, err
	}
	if rp.isNil {
		return nil, nil
	}
	if rp.kind != replyArray {
		return nil, fmt.Errorf("vtvalkey: expected array, got %d", rp.kind)
	}
	if len(rp.items)%2 != 0 {
		return nil, fmt.Errorf("vtvalkey: odd number of elements for map")
	}
	m := make(map[string]string, len(rp.items)/2)
	for i := 0; i < len(rp.items); i += 2 {
		m[rp.items[i].str] = rp.items[i+1].str
	}
	return m, nil
}
