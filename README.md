### Super simplistic implementation of Redis like cache on top of AVL tree in GO

Features:
- GET,SET,UNSET,NUMEQUALTO are all log(n)
- There is client side transaction support. Meaning Rollback transaction log is stored on the client. Every operation is committed to server and in case of rollback client will send a rollback log in one shot back to server. Nested transactions are supported.


note:
 - Transactions don't lock the variables being modified. In the event of rollback, values will be reset to the value at the moment of first operation on that variable inside transaction.
 - Hash based key value storage (like redis) is probably a better option for read heavy usages. But they don't guarantee a constant time insert (in case insert causes a rehash)
 - locking is at tree level right now and happens both for read and write. TODO is to make key value pair locks and support no-locked reads.

To run:
- Server: go to server folder and run
```
$ go run server.go
```
- Client: go to client folder and run
```
$ go run client.go
```