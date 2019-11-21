package file;

import constant.PathConstants;
import id.Id;
import util.ByteUtils;
import util.ErrorCode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class AlphaFileManager implements IFileManager {
    private AlphaFileManagerId fileManagerId;

    // file manager meta info
    private static class Meta implements Serializable {
        private final Map<FieldId, AlphaFileId> fileMap;

        private Meta(Map<FieldId, AlphaFileId> fileMap) {
            this.fileMap = fileMap;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            for (FieldId fieldId : fileMap.keySet()) {
                builder.append("field id: ");
                builder.append(fieldId.getId());
                builder.append(", file id: ");
                builder.append(fileMap.get(fieldId).getId());
                builder.append("\n");
            }
            return builder.toString();
        }
    }

    // create new file manager
    public AlphaFileManager() {
        this.fileManagerId = getNewManagerId();

        // initialize file manager directory
        init();

        // initialize new meta file with empty file map
        writeMeta(new Meta(new HashMap<>()));
        AlphaFileManagerController.addManager(this.fileManagerId);
    }

    // get existing file manager
    public AlphaFileManager(AlphaFileManagerId fileManagerId) {
        if (null == fileManagerId)
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_ARGUMENT);

        this.fileManagerId = fileManagerId;
    }

    // get id for new file manager,  add 1 to id count
    private AlphaFileManagerId getNewManagerId() {
        File file = new File(PathConstants.FILE_MANAGER_PATH, PathConstants.FILE_MANAGER_ID_COUNT);
        long newFileManagerIdNum;

        try {
            BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file));

            // read long-type file manager id count
            byte[] bytes = new byte[Long.BYTES];
            if (inputStream.read(bytes) != bytes.length)
                throw new ErrorCode(ErrorCode.INVALID_FILE_MANAGER_ID);
            inputStream.close();

            // increase id count
            newFileManagerIdNum = ByteUtils.bytesToLong(bytes) + 1;
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
        }

        try {
            // update id count file
            BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));
            byte[] bytes = ByteUtils.longToBytes(newFileManagerIdNum);
            outputStream.write(bytes);
            outputStream.close();
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
        }

        return new AlphaFileManagerId(PathConstants.FILE_MANAGER_PREFIX + newFileManagerIdNum);
    }

    // write serialized meta info into meta file
    private void writeMeta(Meta meta) {
        File file = new File(PathConstants.FILE_MANAGER_PATH, fileManagerId.getId() + PathConstants.META_SUFFIX);

        try {
            ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(file));
            outputStream.writeObject(meta);
            outputStream.close();
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
        }
    }

    // get meta object from meta file
    private Meta readMeta() {
        File file = new File(PathConstants.FILE_MANAGER_PATH, fileManagerId.getId() + PathConstants.META_SUFFIX);

        try {
            ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(file));
            Meta meta = (Meta)inputStream.readObject();
            inputStream.close();
            return meta;
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
        } catch (ClassNotFoundException e) {
            throw new ErrorCode(ErrorCode.FILE_MANAGER_META_FILE_INVALID);
        }
    }

    // initialize file manager directory
    private void init() {
        File dir = new File(PathConstants.FILE_MANAGER_PATH, fileManagerId.getId());
        if (!dir.mkdir())
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, dir.getPath());
    }

    @Override
    public IFile getFile(Id id) {
        if (!AlphaFileManagerController.isServing(fileManagerId)) {
            throw new ErrorCode(ErrorCode.FILE_MANAGER_NOT_SERVING, fileManagerId.getId());
        }

        if (null == id)
            throw new ErrorCode(ErrorCode.NULL_FIELD_ID);
        if (id.getClass() != FieldId.class)
            throw new ErrorCode(ErrorCode.INVALID_FIELD_ID_ARG);

        FieldId fieldId = (FieldId) id;

        Meta meta = readMeta();
        Map<FieldId, AlphaFileId> fileMap = meta.fileMap;

        if (!fileMap.containsKey(fieldId))
            throw new ErrorCode(ErrorCode.UNKNOWN_FIELD_ID, fieldId.getId());
        else
            return new AlphaFile(this.fileManagerId, fileMap.get(fieldId));
    }

    @Override
    public IFile newFile(Id id) {
        if (!AlphaFileManagerController.isServing(fileManagerId)) {
            throw new ErrorCode(ErrorCode.FILE_MANAGER_NOT_SERVING, fileManagerId.getId());
        }

        if (null == id)
            throw new ErrorCode(ErrorCode.NULL_FIELD_ID);

        if (id.getClass() != FieldId.class)
            throw new ErrorCode(ErrorCode.INVALID_FIELD_ID_ARG);

        FieldId fieldId = (FieldId) id;

        AlphaFile file = new AlphaFile(this.fileManagerId);

        Meta meta = readMeta();
        meta.fileMap.put(fieldId, file.getFileId());
        writeMeta(meta);

        return file;
    }

    @Override
    public String getPath() {
        return PathConstants.FILE_MANAGER_PATH + "/" + fileManagerId.getId();
    }

    @Override
    public Id getManagerId() {
        return fileManagerId;
    }
}
