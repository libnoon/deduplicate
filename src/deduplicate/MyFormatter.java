package deduplicate;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class MyFormatter extends Formatter {

    @Override
    public String format(LogRecord record) {
	String dateString = String.format("%tY-%tm-%td", record.getMillis(), record.getMillis(), record.getMillis());
	String timeString = String.format("%tH:%tM:%tS", record.getMillis(), record.getMillis(), record.getMillis());
	return String.format("%s %s: %s: %s %s: %s%n", dateString, timeString, record.getLevel(),
		record.getSourceClassName(), record.getSourceMethodName(), record.getMessage());
    }

}
