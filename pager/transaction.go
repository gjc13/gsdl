package pager

import (
	"fmt"
	"log"
)

const ABORT_RETRY int = 10

type Transactioner interface {
	StartTransaction(filename string)
	EndTransaction() error
	AbortTransaction()
}

type TransactionReader interface {
	StartTransaction(filename string)
	EndTransaction() error
	ReadPage(pgNumber uint32) ([]byte, error)
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

func (e *WriteTransactionError) Error() string {
	return fmt.Sprintf("%s: %s", e.filename, e.msg)
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
}

func (transaction *ReadTransaction) ReadPage(pgNumber uint32) ([]byte, error) {
	return transaction.pager.ReadPage(pgNumber)
}

func (transaction *WriteTransaction) StartTransaction(filename string) {
	transaction.filename = filename
	transaction.pager = getPagerManager().OpenPager(filename,
		func(filename string, pgData []byte, pgNumber uint32) {
			transaction.writeBackPage(filename, pgData, pgNumber)
		})
	getLockManger().AcquireLockExlusive(filename)
}

func (transaction *WriteTransaction) writeBackPage(filename string, pgData []byte, pgNumber uint32) {
	if transaction.aborted {
		return
	}
	//TODO write data to journal
	err := writePageWithAppend(filename, pgData, pgNumber)
	if err != nil {
		transaction.AbortTransaction()
	}
}

func (transaction *WriteTransaction) Sync() error {
	return transaction.pager.SyncAllToDisk()
}

func (transaction *WriteTransaction) EndTransaction() error {
	defer getLockManger().ReleaseLockExlusive(transaction.filename)
	defer getPagerManager().ClosePager(transaction.filename)
	if !transaction.aborted {
		err := transaction.pager.SyncAllToDisk()
		if err != nil {
			for i := 0; i < ABORT_RETRY; i++ {
				if transaction.abortTransaction() == nil {
					return err
				}
			}
			log.Panicf("Cannot abort write transaction even when tried recovery for file %s", transaction.filename)
		}
	}
	return nil
	//TODO add delete journal file here after journal module is done
}

func (transaction *WriteTransaction) AbortTransaction() {
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
		return nil, &WriteTransactionError{transaction.filename, "cannot write back"}
	}
	return data, err
}

func (transaction *WriteTransaction) WritePage(pgNumber uint32, page []byte) error {
	transaction.pager.WritePage(pgNumber, page)
	if transaction.aborted {
		return &WriteTransactionError{transaction.filename, "cannot write back"}
	}
	return nil
}
