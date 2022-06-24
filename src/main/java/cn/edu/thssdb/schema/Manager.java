package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.FileIOException;
import cn.edu.thssdb.parser.SQLHandler;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.common.Global;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// TODO: add lock control
// TODO: complete readLog() function according to writeLog() for recovering transaction

public class Manager {
  private HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  public Database currentDatabase;
  public ArrayList<Long> currentSessions;
  public ArrayList<Long> waitSessions;
  public static SQLHandler sqlHandler;
  public HashMap<Long, ArrayList<String>> x_lockDict;
  public HashMap<Long, ArrayList<String>> s_lockDict;

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    // TODO: init possible additional variables
    databases = new HashMap<>();
    currentDatabase = null;
    sqlHandler = new SQLHandler(this);
    x_lockDict = new HashMap<>();
    s_lockDict = new HashMap<>();
    currentSessions = new ArrayList<>();
    waitSessions = new ArrayList<>();
    File managerFolder = new File(Global.DBMS_DIR + File.separator + "data");
    if(!managerFolder.exists())
      managerFolder.mkdirs();
    this.recover();
  }

  public void deleteDatabase(String databaseName) {
    try {
      lock.writeLock().lock();
      if (!databases.containsKey(databaseName))
        throw new DatabaseNotExistException(databaseName);
      Database database = databases.get(databaseName);
      database.dropDatabase();
      databases.remove(databaseName);
//      persist(); // ?
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void switchDatabase(String databaseName) {
    try {
      lock.readLock().lock();
      if (!databases.containsKey(databaseName))
        throw new DatabaseNotExistException(databaseName);
      currentDatabase = databases.get(databaseName);
    } finally {
      lock.readLock().unlock();
    }
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();
    private ManagerHolder() {

    }
  }

  public Database getCurrentDatabase(){return currentDatabase;}

  // utils:

  // Lock example: quit current manager
  public void quit() {
    try {
      lock.writeLock().lock();
      for (Database database : databases.values())
        database.quit();
      persist();
      databases.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Database get(String databaseName) {
    try {
      lock.readLock().lock();
      if (!databases.containsKey(databaseName))
        throw new DatabaseNotExistException(databaseName);
      return databases.get(databaseName);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void createDatabaseIfNotExists(String databaseName) {
    try {
      lock.writeLock().lock();
      if (!databases.containsKey(databaseName))
        databases.put(databaseName, new Database(databaseName));
      if (currentDatabase == null) {
        try {
          lock.readLock().lock();
          if (!databases.containsKey(databaseName))
            throw new DatabaseNotExistException(databaseName);
          currentDatabase = databases.get(databaseName);
        } finally {
          lock.readLock().unlock();
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void persist() {
    try {
      FileOutputStream fos = new FileOutputStream(Manager.getManagerDataFilePath());
      OutputStreamWriter writer = new OutputStreamWriter(fos);
      for (String databaseName : databases.keySet())
        writer.write(databaseName + "\n");
      writer.close();
      fos.close();
    } catch (Exception e) {
      throw new FileIOException(Manager.getManagerDataFilePath());
    }
  }

  public void persistDatabase(String databaseName) {
    try {
      lock.writeLock().lock();
      Database database = databases.get(databaseName);
      database.quit();
      persist();
    } finally {
      lock.writeLock().unlock();
    }
  }


  // Log control and recover from logs.
  public void writeLog(String statement) {
    String logFilename = this.currentDatabase.getDatabaseLogFilePath();
    try {
      FileWriter writer = new FileWriter(logFilename, true);
      writer.write(statement + "\n");
      writer.close();
    } catch (Exception e) {
      throw new FileIOException(logFilename);
    }
  }

  // TODO: read Log in transaction to recover.
  public void readLog(String databaseName) {
    Database database = databases.get(databaseName);
    String filename = database.getDatabaseLogFilePath();
    File file = new File(filename);
    if (file.exists() && file.isFile()) {
      System.out.println("Reading Log...");
      sqlHandler.evaluate("use " + databaseName, 0); // session?
      int lastCmd = 0;
      ArrayList<String> lines = new ArrayList<>();
      ArrayList<Integer> mSessionInTransactions = new ArrayList<>();
      ArrayList<Integer> mSessionCommitted = new ArrayList<>();
      try(InputStreamReader reader = new InputStreamReader(new FileInputStream(file));
          BufferedReader bufferedReader = new BufferedReader(reader)) {
        String line;
        int index = 0;
        while((line = bufferedReader.readLine()) != null) {
          if (line.equals("begin transaction")) {
            mSessionInTransactions.add(index);
          } else
          if (line.equals("commit")) {
            mSessionCommitted.add(index);
          }
          lines.add(line);
          index++;
        }
        if (mSessionInTransactions.size() == mSessionCommitted.size()) {
          lastCmd = lines.size() - 1;
        } else {
          lastCmd = mSessionInTransactions.get(mSessionInTransactions.size() - 1);
        }
        for (int i = 0; i <= lastCmd; i++) {
          sqlHandler.evaluate(lines.get(i), 0); // 0?
        }
        System.out.println("Read logs completed. Read " + (lastCmd + 1) + " logs");
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
      try(FileWriter writer = new FileWriter(filename)) {
        writer.write("");
      } catch (IOException e) {
        e.printStackTrace();
      }
      try(FileWriter writer = new FileWriter(filename, true)) {
        for (int i = 0; i <= lastCmd; i++) {
          writer.write(lines.get(i) + "\n");
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public void recover() {
    File managerDataFile = new File(Manager.getManagerDataFilePath());
    if (!managerDataFile.isFile()) return;
    try {
      System.out.println("??!! try to recover manager");
      InputStreamReader reader = new InputStreamReader(new FileInputStream(managerDataFile));
      BufferedReader bufferedReader = new BufferedReader(reader);
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        System.out.println("??!!" + line);
        createDatabaseIfNotExists(line);
        readLog(line);
      }
      bufferedReader.close();
      reader.close();
    } catch (Exception e) {
      throw new FileIOException(managerDataFile.getName());
    }
  }

  // Get positions
  public static String getManagerDataFilePath(){
    return Global.DBMS_DIR + File.separator + "data" + File.separator + "manager";
  }

  public ArrayList<Long> getSessionsInLocks() {
    return waitSessions;
  }

  public ArrayList<Long> getSessionsInTransactions() {
    return currentSessions;
  }

  public HashMap<Long, ArrayList<String>> getxLocks() {
    return x_lockDict;
  }

}
