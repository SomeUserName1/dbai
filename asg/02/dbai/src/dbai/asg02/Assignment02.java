package dbai.asg02;

public interface Assignment02 {
	boolean blockFree(byte[] bitmap, int blockNr);

	void markBlock(byte[] bitmap, int blockNr, boolean free);
}
