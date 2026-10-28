package winfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Escritura local en Windows sin winutils ni la biblioteca nativa de Hadoop.
 * Los permisos POSIX no se aplican.
 */
public class WinLocalFileSystem extends RawLocalFileSystem {
    @Override
    public void setPermission(Path path, FsPermission permission) throws IOException {
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        File archivo = pathToFile(path);
        if (!archivo.exists()) {
            throw new FileNotFoundException(path.toString());
        }
        return new FileStatus(
            archivo.length(),
            archivo.isDirectory(),
            1,
            4096L,
            archivo.lastModified(),
            path
        );
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        File archivo = pathToFile(path);
        if (!archivo.exists()) {
            throw new FileNotFoundException(path.toString());
        }
        if (archivo.isFile()) {
            return new FileStatus[] { getFileStatus(path) };
        }
        File[] hijos = archivo.listFiles();
        if (hijos == null) {
            return new FileStatus[0];
        }
        FileStatus[] estados = new FileStatus[hijos.length];
        for (int indice = 0; indice < hijos.length; indice++) {
            estados[indice] = getFileStatus(new Path(path, hijos[indice].getName()));
        }
        return estados;
    }
}
