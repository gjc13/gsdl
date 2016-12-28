package pager

import (
	"fmt"
	"io"
	"math"
	"os"
)

const PGSIZE uint32 = 4096

type PageIOError struct {
	filename string
	msg      string
}

func (err *PageIOError) Error() string {
	return fmt.Sprintf("%s: %s", err.filename, err.msg)
}

func loadPage(filename string, pgNumber uint32) ([]byte, error) {
	file, err1 := os.Open(filename)
	if err1 != nil {
		return nil, err1
	}
	defer file.Close()
	data := make([]byte, PGSIZE)
	_, err2 := file.ReadAt(data, int64(PGSIZE*pgNumber))
	if err2 != nil {
		return nil, err2
	}
	return data, nil
}

func writePage(filename string, data []byte, pgNumber uint32) error {
	if len(data) != int(PGSIZE) {
		return &PageIOError{filename, "write data length can only be a page"}
	}
	file, err1 := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0660)
	if err1 != nil {
		return err1
	}
	defer file.Close()
	_, err2 := file.WriteAt(data, int64(PGSIZE*pgNumber))
	if err2 != nil {
		err3 := appendPage(filename, pgNumber)
		return err3
	}
	return nil
}

func writePageWithAppend(filename string, data []byte, pgNumber uint32) error {
	err1 := writePage(filename, data, pgNumber)
	if err1 == io.EOF {
		err2 := appendPage(filename, pgNumber)
		if err2 != nil {
			return err2
		}
		return writePage(filename, data, pgNumber)
	}
	return nil
}

func appendPage(filename string, pgNumber uint32) error {
	file, err1 := os.OpenFile(filename, os.O_WRONLY, 0660)
	if err1 != nil {
		return err1
	}
	defer file.Close()
	info, err2 := file.Stat()
	if err2 != nil {
		return err2
	}
	nowSize := info.Size()
	if nowSize/int64(PGSIZE) > math.MaxUint32 {
		return &PageIOError{filename, "number of pages bigger than uint32 limit"}
	}
	var nowPgNumber uint32 = uint32(nowSize/int64(PGSIZE) - 1)
	if pgNumber <= nowPgNumber {
		return nil
	}
	_, err3 := file.Seek(0, os.SEEK_END)
	if err3 != nil {
		return err3
	}
	data := make([]byte, PGSIZE)
	for i := nowPgNumber; i < pgNumber; i++ {
		_, err4 := file.Write(data)
		if err4 != nil {
			return err4
		}
	}
	return nil
}

func shrinkFile(filename string, targetNumberPages uint32) error {
	file, err1 := os.OpenFile(filename, os.O_WRONLY, 0660)
	if err1 != nil {
		return err1
	}
	defer file.Close()
	return file.Truncate(int64(targetNumberPages * PGSIZE))
}

func createFile(filename string, targetNumberPages uint32) error {
	file, err1 := os.Create(filename)
	if err1 != nil {
		file.Close()
	}
	return err1
}
