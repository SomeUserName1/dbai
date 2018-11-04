package dbai1819.asg01;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.ByteBuffer;
import static java.lang.StrictMath.toIntExact;

/**
 * Class for input/output methods.
 * 
 * @author Christian Gruen, DBIS, University of Konstanz
 */
class IO {
  /**
   * Writes an empty file with the specified size.
   * @param path file path
   * @param len length in bytes
   * @throws IOException I/O exception
   */
  static void create(final Path path, final long len) throws IOException {
    FileOutputStream fos = new FileOutputStream(path.toFile());
    // open file ByteChannel for new file rw
    FileChannel channel = fos.getChannel();
    // alloc a new buffer
    ByteBuffer buffer = ByteBuffer.allocate(1);
    // to write one byte of zeros
    buffer.put((byte)0);
    // at the end of our desired file size to grow it
    channel.position(len-1);
    channel.write(buffer);
    // clear the buffer and close the channel
    channel.force(true);
    channel.close();
    fos.close();
  }

  /**
   * Reads the contents of the specified file.
   * @param path file path
   * @return byte array
   * @throws IOException I/O exception
   */
  static byte[] read(final Path path) throws IOException {
      byte[] result;
      FileInputStream fis = new FileInputStream(path.toFile());
    // Open file ByteChannel
    FileChannel channel = fis.getChannel();

    // Create byte array, currently errors if the channel contains more than 2 GB.
    try {
      result = new byte[toIntExact(channel.size())];
    } catch (ArithmeticException e){
        System.out.println("Please read a smaller portion!");
        result = new byte[Integer.MAX_VALUE];
    }
    // Wrap it into a buffer
    ByteBuffer buffer = ByteBuffer.wrap(result);
    // read whats in the file
    channel.read(buffer);
    buffer.clear();
    channel.close();
    fis.close();
    // return the array
    return result;
  }

  /**
   * Deletes the specified file.
   * @param path file path
   * @throws IOException I/O exception
   */
  static void delete(final Path path) throws IOException {
    Files.deleteIfExists(path);
  }
}
