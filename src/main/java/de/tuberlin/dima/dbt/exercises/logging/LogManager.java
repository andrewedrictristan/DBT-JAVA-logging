package de.tuberlin.dima.dbt.exercises.logging;

import java.util.*;

public class LogManager {

    /**
     * Buffer Manager dependency.
     * <p>
     * The Log Manager needs to interact with the Buffer Manager during
     * checkpointing and recovery.
     */
    protected BufferManager bufferManager;

    /**
     * The current log sequence number.
     */
    protected int currentLsn = 0;

    /**
     * The list of log records.
     */
    protected List<LogRecord> logRecords;

    /**
     * The active transaction table.
     * <p>
     * The transaction table maps transaction IDs to log sequence numbers.
     */
    protected Map<Integer, Integer> transactions;

    /**
     * The dirty page table.
     * <p>
     * The dirty page table maps page IDs to log sequence numbers.
     */
    protected Map<Integer, Integer> dirtyPages;
    boolean startingCheckpoint = false;
    boolean endingCheckpoint = false;
    /////
    ///// Functions that append log records
    /////

    /**
     * Add a BEGIN_OF_TRANSACTION entry to the log.
     *
     * @param transactionId The transaction ID.
     * @throws A LogManagerException if a transaction with the ID already exists.
     */
    public void beginTransaction(int transactionId) {
        // TODO Append BEGIN_OF_TRANSACTION entry to log
        int lsn = currentLsn;
        currentLsn += 1;


        for (int i = 0; i < logRecords.size(); i++) {
            if (logRecords.get(i).getTransactionId() != null) {
                if (logRecords.get(i).getTransactionId() == transactionId)
                    throw new LogManagerException("Existed Transaction ID");
            }
        }

        if (startingCheckpoint != true){
            transactions.put(transactionId, lsn);
        }

        this.logRecords.add(new LogRecord(lsn, transactionId));


    }

    /**
     * Add an UPDATE entry to the log.
     *
     * @param transactionId The transaction ID.
     * @param pageId        The page ID.
     * @param elementId     The element ID.
     * @param oldValue      The old value.
     * @param newValue      The new value.
     * @throws A LogManagerException if the transaction with the ID does not exist.
     */
    public void update(int transactionId, int pageId, int elementId,
                       String oldValue, String newValue) {
        LogRecord rec = null;


        int y = this.logRecords.size() - 1;
        int lsn = this.currentLsn;
        if (y >= 0) {


            do {
                if (this.logRecords.get(y).getTransactionId() == null) {
                    y--;
                    continue;
                }
                if (this.logRecords.get(y).getTransactionId() == transactionId) {
                    rec = this.logRecords.get(y);

                    this.logRecords.add(new LogRecord(lsn, transactionId, rec.getLogSequenceNumber(), pageId, elementId, oldValue, newValue));
                    break;
                }
                y--;
            } while (y >= 0);
        }
        if (rec == null)
            throw new LogManagerException("this ID has not been existed");




        if (startingCheckpoint != true){
            transactions.put(transactionId, lsn);

            if (this.dirtyPages.get(pageId) == null){
                this.dirtyPages.put(pageId, lsn);
            } else if (this.dirtyPages.get(pageId) != null){
                if (lsn < this.dirtyPages.get(pageId))
                    this.dirtyPages.put(pageId, lsn);
            }

        }
        currentLsn = currentLsn +  1;


    }

    /**
     * Add a COMMIT entry to the log.
     *
     * @param transactionId The transaction ID.
     * @throws A LogManagerException if the transaction with the ID does not exist.
     */
    public void commit(int transactionId) {
        boolean foundedRecord = false;
        int TableLogRecordSize = this.logRecords.size();
        int x = 0;

        for (; x < TableLogRecordSize; x++){
            if (this.logRecords.get(x).getTransactionId() == null) continue;
            if (transactionId == this.logRecords.get(x).getTransactionId()){
                foundedRecord = true;
            }
        }

        if (!foundedRecord) {
            throw new LogManagerException("This given transaction ID is not existed.");
        }

        Integer previousLsn = null;
        int y = 0;
        int lsn = this.currentLsn;
        for (; y < TableLogRecordSize; y++) {

            if (this.logRecords.get(y).getTransactionId() == null) continue;
            if (transactionId == this.logRecords.get(y).getTransactionId()) {
                previousLsn = this.logRecords.get(y).getLogSequenceNumber();
            }
        }
        if(previousLsn != null){
            this.logRecords.add(new LogRecord(lsn, LogRecordType.COMMIT, transactionId, previousLsn));
            this.transactions.remove(transactionId);
        }

        this.currentLsn += 1;
    }

    /**
     * Add a END_OF_TRANSACTION entry to the log.
     *
     * @param transactionId The transaction ID.
     * @throws A LogManagerException if the transaction with the ID does not exist.
     */
    public void endTransaction(int transactionId) {
        boolean foundedRecord = false;
        Integer previousLsn = null;
        int lsn = this.currentLsn;
        int TableLogRecordSize = this.logRecords.size();
        int x = 0;

        for (; x < TableLogRecordSize; x++){
            if (this.logRecords.get(x).getTransactionId() == null) continue;
            if (transactionId == this.logRecords.get(x).getTransactionId()){
                foundedRecord = true;
            }
        }

        if (!foundedRecord) {
            throw new LogManagerException("This given transaction ID is not existed.");
        }

        int y = 0;
        for (; y < TableLogRecordSize; y++) {

            if (this.logRecords.get(y).getTransactionId() == null) continue;
            if (transactionId == this.logRecords.get(y).getTransactionId()) {
                previousLsn = this.logRecords.get(y).getLogSequenceNumber();
            }
        }

        this.logRecords.add(new LogRecord(lsn, LogRecordType.END_OF_TRANSACTION, transactionId, previousLsn));
        this.currentLsn += 1;
    }

    /////
    ///// Functions that implement checkpointing
    /////

    /**
     * Add a BEGIN_OF_CHECKPOINT entry to the log.
     */
    int n;
    public void beginCheckpoint() {
        startingCheckpoint= true;

        int lsn = currentLsn;
        n = lsn;
        this.logRecords.add(new LogRecord(lsn, LogRecordType.BEGIN_OF_CHECKPOINT));
        this.currentLsn += 1;

    }

    /**
     * Add a END_OF_CHECKPOINT entry to the log and write checkpointing data to disk.
     * <p>
     * Should write:
     * <ul>
     * <li>The LSN of the BEGIN_OF_CHECKPOINT entry.</li>
     * <li>The transaction table.</li>
     * <li>The dirty page table.</li>
     * </ul>
     */
    public void endCheckpoint() {
        startingCheckpoint= false;
        int lsn = currentLsn;

        this.logRecords.add(new LogRecord(lsn, LogRecordType.END_OF_CHECKPOINT));

        int i = this.logRecords.size() - 1;

        this.bufferManager.writeBeginningOfCheckpoint(n);
        int transactionSize = this.transactions.size();
        int dirtyPageSize= this.dirtyPages.size();

        if (transactionSize > 0) {
            this.bufferManager.writeTransactionTable(this.transactions);
        }
        if (dirtyPageSize > 0) {
            this.bufferManager.writeDirtyPageTable(this.dirtyPages);
        }

        this.currentLsn += 1;
    }

    /////
    ///// Functions that implement recovery
    /////

    /**
     * Analyse an undo/redo log starting from a specific LSN and update the
     * transaction and dirty page tables.
     * <p>
     * The transaction and dirty page tables, which are passed to this function,
     * should be updated in-place.
     *
     * @param logRecords      The complete undo/redo log.
     * @param checkPointedLsn The LSN at which the analysis path should start.
     * @param transactions    The transaction table.
     * @param dirtyPages      The dirty page table.
     */
    public void analysisPass(List<LogRecord> logRecords, int checkPointedLsn,
                             Map<Integer, Integer> transactions,
                             Map<Integer, Integer> dirtyPages) {

        for (int x = 0;x < logRecords.size(); x++ ) {
            if (logRecords.get(x).getLogSequenceNumber() >= checkPointedLsn) {
                if (logRecords.get(x).getType() == LogRecordType.UPDATE) {
                    if (!dirtyPages.containsKey(logRecords.get(x).getPageId()))
                        dirtyPages.put(logRecords.get(x).getPageId(), logRecords.get(x).getLogSequenceNumber());
                }
                if (logRecords.get(x).getType() != LogRecordType.BEGIN_OF_CHECKPOINT  && logRecords.get(x).getType() != LogRecordType.END_OF_CHECKPOINT) {
                    transactions.put(logRecords.get(x).getTransactionId(), logRecords.get(x).getLogSequenceNumber());
                }
                if (logRecords.get(x).getType() == LogRecordType.END_OF_TRANSACTION  ) {
                    transactions.remove(logRecords.get(x).getTransactionId());
                }
            }
        }
    }

    /**
     * Perform the redo pass for the given log records.
     * <p>
     * An UPDATE statement that has to be redone should call
     * bufferManager.writeElement with the required page ID, element ID, and
     * the value that should be writen.
     *
     * @param logRecords The complete undo/redo log.
     * @param dirtyPages The dirty page table constructed during the analysis
     *                   path.
     */
    public void redoPass(List<LogRecord> logRecords,
                         Map<Integer, Integer> dirtyPages) {



        if (dirtyPages.size() > 0){

            Collection<Integer> values = dirtyPages.values();
            int s = Collections.min(values);

            for (int i= s;i < logRecords.size(); i++) {
                if (logRecords.get(i).getType() == LogRecordType.UPDATE) {
                    if (dirtyPages.containsKey(logRecords.get(i).getPageId()) == false) {
                        continue;
                    } else {
                        if (dirtyPages.get(logRecords.get(i).getPageId()) > logRecords.get(i).getLogSequenceNumber()) {
                            continue;
                        }
                    }
                    if (bufferManager.getPageLsn(logRecords.get(i).getPageId()) >= logRecords.get(i).getLogSequenceNumber()) {
                        continue;
                    }
                    bufferManager.writeElement(logRecords.get(i).getPageId(), logRecords.get(i).getElementId(), logRecords.get(i).getNewValue());
                }
            }
        } else {
            return;
        }
    }

    /**
     * Perform the undo pass for the given log entries.
     * <p>
     * An UPDATE statement that has to be undone should call
     * bufferManager.writeElement with the required page ID, element ID, and
     * the value that should be writen.
     *
     * @param logRecords   The complete undo/redo log.
     * @param transactions The transaction table constructed during the
     *                     analysis path.
     */
    public void undoPass(List<LogRecord> logRecords,
                         Map<Integer, Integer> transactions) {
        // Check transaction table
        for (Integer key : transactions.keySet()) {
            for (int i = logRecords.size()-1; i >= 0; i--) {
                //log Record table
                LogRecord logRecordOriginal = logRecords.get(i);
                //compare transaction table and log record
                if (logRecordOriginal.getType() != LogRecordType.UPDATE) continue;
                else if (key.intValue() == logRecordOriginal.getTransactionId() ){
                    bufferManager.writeElement(logRecordOriginal.getPageId(), logRecordOriginal.getElementId(), logRecordOriginal.getOldValue());
                }else {
                    continue;
                }
            }
        }
    }

    /**
     * Constructs an empty undo/redo log.
     * <p>
     * DO NOT CHANGE THIS CODE.
     *
     * @param bufferManager Dependency on a Buffer Manager.
     */
    public LogManager(BufferManager bufferManager) {
        this(bufferManager, 0, new ArrayList<>(), new HashMap<>(),
                new HashMap<>());
    }

    /**
     * Constructs an undo/redo log based on existing log records and transaction
     * and dirty page table.
     * <p>
     * DO NOT CHANGE THIS CODE.
     *
     * @param bufferManager Dependency on a Buffer Manager.
     * @param currentLsn    The current LSN. New entries will be appended to the
     *                      log starting with this LSN.
     * @param logRecords    A list of existing log records.
     * @param transactions  A transaction table.
     * @param dirtyPages    A dirty page table.
     */
    public LogManager(BufferManager bufferManager, int currentLsn,
                      List<LogRecord> logRecords,
                      Map<Integer, Integer> transactions,
                      Map<Integer, Integer> dirtyPages) {
        this.bufferManager = bufferManager;
        this.currentLsn = currentLsn;
        this.logRecords = logRecords;
        this.transactions = transactions;
        this.dirtyPages = dirtyPages;
    }

    public List<LogRecord> getLogRecords() {
        return logRecords;
    }

}
