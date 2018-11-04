package dbai.asg02;

public class Assignment02Group00 implements Assignment02 {

	@Override
	public boolean blockFree(byte[] bitmap, int blockNr) {
		// logical right shift doesn't care about signs and simply fills byte with zeros from the left side i.e.
		// 11111110 >>> 1 = 01111111 whereas 11111110 >> 1 = 11111111 (twos complement: -2 will be -1)

        // blockNr / 8 = byte for block region, blockNr mod 8 = bitNrInByteRegion
        // => e.g. 0010 1101 & 0000 0010 != 0 for block 1
        return (bitmap[blockNr >>> 3] & (1 << (blockNr & 7))) != 0;
	}

	@Override
	public void markBlock(byte[] bitmap, int blockNr, boolean free) {
        // 1011 0011 OR 0000 1000 sets exactly the one bit; 1011 0011 AND 111 1011 un-sets exactly the zero bit
		bitmap[blockNr >>> 8] = free ? (byte)(bitmap[blockNr >>> 3] | (1 << (blockNr & 7)))
				: (byte)(bitmap[blockNr >>> 3] & ~(1 << (blockNr & 7)));
	}

}
