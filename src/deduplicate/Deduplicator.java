package deduplicate;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class Deduplicator {

  public static void main(String[] args) {
    Deduplicator dedup = new Deduplicator(args);
    dedup.run();
  }

  private List<Path> topDirectories = new ArrayList<>();
  FileDatabase fileDatabase = new FileDatabase();

  private Deduplicator(String[] args) {
    for (String arg: args) {
      topDirectories.add(Paths.get(arg));
    }
  }
  
  private class MyFileVisitor extends SimpleFileVisitor<Path> {

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
      if (attrs.isRegularFile()) {
        recordFile(file);
      }
      return FileVisitResult.CONTINUE;
    }

  }

  private void run() {
    int maxDepth = Integer.MAX_VALUE;
    MyFileVisitor visitor = new MyFileVisitor();
    for (Path file: topDirectories) {
      try {
        Files.walkFileTree(file, EnumSet.of(FileVisitOption.FOLLOW_LINKS), maxDepth, visitor);
      }
      catch (IOException exception) {
        System.out.println(String.format("cannot walk in tree %s: %s%n", file, exception));
      }
    }
    
    fileDatabase.readFilesAndFindDuplicates();
  }

  private void recordFile(Path file) {
    fileDatabase.add(file);
  }

}
