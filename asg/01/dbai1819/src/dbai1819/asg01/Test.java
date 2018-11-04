package dbai1819.asg01;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import dbai1819.Performance;

/**
 * Test class.
 * 
 * @author Christian Gruen, DBIS, University of Konstanz
 */
class Test {
  /**
   * Main method.
   * @param args command-line arguments (ignored)
   * @throws IOException I/O exception
   */
  public static void main(String[] args) throws IOException {
    // measure performance
    final Performance perf = new Performance();
    final Path file = Paths.get("test.dat");
    
    // create 100mb file
    final long size = 99999999;
    IO.create(file, size);
    System.out.println("Creating file: " + perf);


      // read file
      IO.read(file);
      System.out.println("Reading file: " + perf);

    // delete file
    IO.delete(file);
    System.out.println("Deleting file: " + perf);
  }
}
