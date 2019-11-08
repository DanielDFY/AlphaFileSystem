package block;

import id.Id;

public interface IBlock {
    Id getIndexId();
    IBlockManager getBlockManager();

    byte[] read();
    int blockSize();
}
