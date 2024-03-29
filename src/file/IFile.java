package file;

import id.Id;

public interface IFile {
    int MOVE_CURR = 0;
    int MOVE_HEAD = 1;
    int MOVE_TAIL = 2;

    Id getFileId();
    IFileManager getFileManager();

    byte[] read(int length);
    void write(byte[] b);
    default long pos() {
        return move(0, MOVE_CURR);
    }
    long move(long offset, int where);

    void close();

    long size();
    void setSize(long newSize);

    void copyTo(IFile dst);
}
