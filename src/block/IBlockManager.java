package block;

import id.Id;

public interface IBlockManager {
    IBlock getBlock(Id indexId);
    IBlock newBlock(byte[] b);
    default IBlock newEmptyBlock(int blockSize) {
        return newBlock(new byte[blockSize]);
    }
    String getPath();
    Id getManagerId();
}
