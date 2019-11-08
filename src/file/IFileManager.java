package file;

import id.Id;

public interface IFileManager {
    IFile getFile(Id fileId);
    IFile newFile(Id fileId);
    String getPath();
    Id getManagerId();
}
