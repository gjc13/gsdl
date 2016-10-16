package pager

import (
	"fmt"
	"log"
)

const ABORT_RETRY int = 10

type Transaction interface {
	StartTransaction(filename string)
	EndTransaction() error
	AbortTransaction()
}

type ReadTransaction struct {
	filename string
	pager    *Pager
}

type WriteTransaction struct {
	filename string
	pager    *Pager
	aborted  bool
}

type WriteTransactionError struct {
	filename string
	msg      string
}

func (*WriteTransactionError) Error() {
	return fmt.Sprintf("%s: %s", filename, msg)
}

func (transaction *ReadTransaction) StartTransaction(filename string) {
	transaction.filename = filename
	transaction.pager = getPagerManager().OpenPager(filename, nil)
	getLockManger().AcquireLockShared(filename)
}

func (transaction *ReadTransaction) EndTransaction() error {
	getPagerManager().ClosePager(transaction.filename)
	getLockManger().ReleaseLockShared(transaction.filename)
	return nil
}

func (transaction *ReadTransaction) AbortTransaction() {
	//For read transaction, no rollback needs to be perfomed
	return nil
}

func (transaction *ReadTransaction) ReadPage(pgNumber uint32) ([]byte, error) {
	return transaction.pager.ReadPage(pgNumber)
}

func (transaction *WriteTransaction) StartTransaction(filename string) error {
	transaction.filename = filename
	transaction.pager = getPagerManager().OpenPager(filename,
		func(filename string, pgData []byte, pgNumber uint32) {
			transaction.writeBackPage(filename, pgNumber, pgData)
		})
	getLockManger().AcquireLockExlusive(filename)
}

func (transaction *WriteTransaction) writeBackPage(filename string, pgData []byte, pgNumber uint32) {
	if transaction.aborted {
		return
	}
	err := writePageWithAppend(filename, pgData, pgNumber)
	if err != nil {
		transaction.AbortTransaction()
	}
}

func (transaction *WriteTransaction) EndTransaction() error {
	defer getLockManger().ReleaseLockExlusive(transaction.filename)
	err := transaction.pager.SyncAllToDisk()
	if err != nil {
		for i := 0; i < ABORT_RETRY; i++ {
			if transaction.abortTransaction() == nil {
				return err
			}
		}
		log.Panicf("Cannot abort write transaction even when tried recovery for file %s", transaction.filename)
	}
	//TODO add delete journal file here after journal module is done
}

func (transaction *WriteTransaction) AbortTransaction() {
	defer getLockManger().ReleaseLockExlusive(transaction.filename)
	for i := 0; i < ABORT_RETRY; i++ {
		if transaction.abortTransaction() == nil {
			return
		}
	}
	log.Panicf("Cannot abort write transaction even when tried recovery for file %s", transaction.filename)
}

func (transaction *WriteTransaction) abortTransaction() error {
	//TODO Add rollback here after the journal module is done
	transaction.aborted = true
	transaction.pager.PurgeCache()
	return nil
}

func (transaction *WriteTransaction) ReadPage(pgNumber uint32) ([]byte, error) {
	data, err := transaction.pager.ReadPage(pgNumber)
	if transaction.aborted {
		return nil, &WriteTransactionError(transaction.filename, "cannot write back")
	}
	return data, err
}

func (transaction *WriteTransaction) WritePage(pgNumber uint32, page []byte) error {
	transaction.pager.WritePage(pgNumber, page)
	if transaction.aborted {
		return nil, &WriteTransactionError(transaction.filename, "cannot write back")
	}
	return nil
}
