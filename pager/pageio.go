package pager

import (
	"fmt"
	"io"
	"math"
	"os"
)

const PGSIZE uint32 = 4096

type PageIOError struct {
	string filename
	string msg
}

func (*PageIOError) Error() {
	return fmt.Sprintf("%s: %s", filename, msg)
}

func loadPage(filename string, pgNumber uint32) ([]byte, error) {
	file, err1 := os.Open(filename)
	if err1 != nil {
		return nil, err1
	}
	defer file.Close()
	var data [PGSIZE]byte
	_, err2 := file.ReadAt(data, PGSIZE*pgNumber)
	if err2 != nil {
		return nil, err2
	}
	return data, nil
}

func writePage(filename string, data []byte, pgNumber uint32) error {
	if len(data) != PGSIZE {
		return &PageIOError{filename, "write data length can only be a page"}
	}
	file, err1 := os.OpenFile(filename, os.O_WRONLY, 0660)
	if err1 != nil {
		return err1
	}
	defer file.Close()
	_, err2 := file.WriteAt(data, PGSIZE*pgNumber)
	if err2 != nil {
		err3 := appendPage(filename, pgNumber)
	}
	return nil
}

func writePageWithAppend(filename string, data []byte, pgNumber uint32) error {
	err1 := writePage(filename, data, pgNumber)
	if err1 == io.EOF {
		err2 := appendPage(string, pgNumber)
		if err2 != nil {
			return err2
		}
		return writePage(filename, data, pgNumber)
	}
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
	if nowSize/PGSIZE > math.MaxUint32 {
		return &PageIOError{filename, "number of pages bigger than uint32 limit"}
	}
	var nowPgNumber uint32 = uint32(nowSize/PGSIZE - 1)
	if pgNumber <= nowPgNumber {
		return nil
	}
	err3 := file.Seek(0, os.SEEK_END)
	if err3 != nil {
		return err3
	}
	data := make([]byte, PGSIZE)
	for i := nowPgNumber; i < pgNumber; i++ {
		err4 := file.Write(data)
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
	return file.Truncate(targetNumberPages * PGSIZE)
}

func createFile(filename string, targetNumberPages uint32) error {
	file, err1 := os.Create(filename)
	if err1 != nil {
		file.Close()
	}
	return err1
}
