package org.camunda.tngp.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.*;

public class FileUtilTest
{

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void shouldDeleteFolder() throws IOException
    {
        final File root = tempFolder.getRoot();

        tempFolder.newFile("file1");
        tempFolder.newFile("file2");
        tempFolder.newFolder("testFolder");

        FileUtil.deleteFolder(root.getAbsolutePath());

        assertThat(root.exists()).isFalse();
    }

    @Test
    public void shouldThrowExceptionForNonExistingFolder()
    {
        final File root = tempFolder.getRoot();

        tempFolder.delete();

        assertThatThrownBy(() ->
        {

            FileUtil.deleteFolder(root.getAbsolutePath());

        }).isInstanceOf(NoSuchFileException.class);
    }

}
