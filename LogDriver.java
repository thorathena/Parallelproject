
import org.apache.hadoop.util.ProgramDriver;

public class LogDriver {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("loganalyzer", LogAnalyzer.class, "Extracts URLs from Apache log files");
			pgd.driver(args);
			exitCode = 0;
		} catch (Throwable e) {
			e.printStackTrace();
		}
		System.exit(exitCode);
	}

}
