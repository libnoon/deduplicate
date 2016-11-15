package deduplicate;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Logger;

public class FileDatabase {

    private static final int BUFFER_SIZE = 1024 * 1024;

    private static final Logger logger = Logger.getLogger("FileDatabase");

    private final List<FileEntry> fileEntries = new ArrayList<>();
    private final Map<String, List<FileEntry>> digestMap = new HashMap<>();

    public void add(Path file) {
	Objects.requireNonNull(file);
	FileEntry fileEntry = new FileEntry(file);
	fileEntries.add(fileEntry);
    }

    private static class FileEntry {
	private Path path;
	private Long inode;
	private String digest;
	private Optional<Long> size = Optional.empty();
	private static final String DIGEST_ALGORITHM = "MD5";

	public FileEntry(Path file) {
	    Objects.requireNonNull(file);
	    this.path = file;
	}

	@Override
	public String toString() {
	    return String.format("FileEntry(%s, %d, %s)", path, getInode(), getDigest());
	}

	public String getDigest() {
	    if (digest == null) {
		try {
		    MessageDigest messageDigest = MessageDigest.getInstance(DIGEST_ALGORITHM);
		    byte[] buffer = new byte[BUFFER_SIZE];
		    try (InputStream inputStream = Files.newInputStream(path);
			    DigestInputStream digestInputStream = new DigestInputStream(inputStream, messageDigest)) {
			while (true) {
			    if (digestInputStream.read(buffer) == -1) {
				break;
			    }
			}
		    }
		    byte[] digestArray = messageDigest.digest();

		    // Taken from
		    // http://stackoverflow.com/questions/9655181/how-to-convert-a-byte-array-to-a-hex-string-in-java
		    digest = new BigInteger(1, digestArray).toString(16);
		    return digest;

		} catch (Exception exc) {
		    throw new RuntimeException(String.format("cannot compute digest of %s", path), exc);
		}
	    } else {
		return digest;
	    }
	}

	public Path getPath() {
	    return path;
	}

	public boolean isDuplicateOf(FileEntry candidate) {
	    Objects.requireNonNull(candidate);
	    if (getInode() == candidate.getInode()) {
		return false;
	    }
	    try {
		try (FileChannel fc1 = FileChannel.open(path, StandardOpenOption.READ);
			FileChannel fc2 = FileChannel.open(candidate.path, StandardOpenOption.READ);) {
		    MappedByteBuffer mbb1 = fc1.map(MapMode.READ_ONLY, 0, fc1.size() - 1);
		    MappedByteBuffer mbb2 = fc2.map(MapMode.READ_ONLY, 0, fc2.size() - 1);
		    if (mbb1.compareTo(mbb2) == 0) {
			return true;
		    } else {
			return false;
		    }
		}
	    } catch (IOException e) {
		throw new RuntimeException(
			String.format("cannot check if files %s and %s are duplicate", path, candidate.path));
	    }
	}

	private long getInode() {
	    if (inode == null) {
		try {
		    inode = (Long) Files.getAttribute(path, "unix:ino");
		    return inode;
		} catch (IOException e) {
		    throw new RuntimeException(String.format("cannot get inode of %s", path), e);
		}
	    } else {
		return inode;
	    }
	}

	private long getSize() throws IOException {
	    if (!size.isPresent()) {
	    } else {
		size = Optional.of(Files.size(path));
	    }
	    return size.get();
	}

    }

    private static class TempDir implements AutoCloseable {
	private Path path;

	public TempDir(Path parent) {
	    Objects.requireNonNull(parent);
	    try {
		this.path = Files.createTempDirectory(parent, null);
	    } catch (IOException e) {
		throw new RuntimeException(String.format("cannot create temporary directory under %s", parent), e);
	    }
	}

	@Override
	public void close() {
	    try {
		Files.delete(path);
	    } catch (IOException e) {
		logger.warning(String.format("%s: cannot delete %s: %s", toString(), path, e));
	    }
	}

	public Path getPath() {
	    return path;
	}

	@Override
	public String toString() {
	    return String.format("TempDir(%s)", path);
	}
    }

    private class TempLink implements AutoCloseable {
	private Path link;
	private Path target;
	private boolean linkStillPresent;

	public TempLink(Path link, Path target) throws IOException {
	    Objects.requireNonNull(link);
	    Objects.requireNonNull(target);
	    this.link = link;
	    this.target = target;
	    Files.createLink(link, target);
	    linkStillPresent = true;
	}

	public void move(Path newPath) throws IOException {
	    Objects.requireNonNull(newPath);
	    Files.move(link, newPath, StandardCopyOption.ATOMIC_MOVE);
	    linkStillPresent = false;
	}

	@Override
	public String toString() {
	    return String.format("TempLink(%s, %s)", link, target);
	}

	@Override
	public void close() throws IOException {
	    if (linkStillPresent) {
		try {
		    Files.delete(link);
		} catch (IOException e) {
		    throw new IOException(String.format("%s: cannot delete %s", toString(), link), e);
		}
	    }
	}

    }

    public void readFilesAndFindDuplicates() {
	logger.info(String.format("Getting the digests of %d files...", fileEntries.size()));
	for (FileEntry fileEntry : fileEntries) {
	    String digest = fileEntry.getDigest();
	    logger.fine(String.format("%s  %s", digest, fileEntry.getPath()));
	    List<FileEntry> list = digestMap.get(digest);
	    if (list == null) {
		list = new ArrayList<FileEntry>();
		digestMap.put(digest, list);
	    }
	    list.add(fileEntry);
	}

	for (Map.Entry<String, List<FileEntry>> entry : digestMap.entrySet()) {
	    List<FileEntry> list = entry.getValue();
	    logger.info(
		    String.format("Checking the %d files which have digest \"%s\"...", list.size(), entry.getKey()));

	    SUBJECT: for (int i = 0; i < list.size(); i++) {
		// If "subject" has a duplicate, then replace the duplicate with
		// a link to the subject.
		FileEntry subject = list.get(i);
		// Try to remove subject as a duplicate of something else.
		for (int j = i + 1; j < list.size(); j++) {
		    FileEntry candidateDuplicate = list.get(j);
		    if (candidateDuplicate == subject) {
			// Don't compare with ourself
			continue;
		    }

		    if (subject.isDuplicateOf(candidateDuplicate)) {
			logger.info(String.format("dup: subject=%s and dup=%s", subject, candidateDuplicate));
			replaceWithHardLink(subject, candidateDuplicate);
			continue SUBJECT;
		    }
		}
	    }
	}
    }

    private void replaceWithHardLink(FileEntry linkTarget, FileEntry toReplace) {
	Objects.requireNonNull(linkTarget);
	Objects.requireNonNull(toReplace);
	try (TempDir tempDir = new TempDir(toReplace.getPath().getParent())) {
	    Path link = tempDir.getPath().resolve("link");

	    try (TempLink tempLink = new TempLink(link, linkTarget.getPath())) {
		tempLink.move(toReplace.getPath());
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	}
    }

}
