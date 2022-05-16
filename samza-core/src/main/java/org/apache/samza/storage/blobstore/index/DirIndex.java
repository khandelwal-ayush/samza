/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.storage.blobstore.index;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Representation of a directory in the blob store
 */
public class DirIndex {
  public static final String ROOT_DIR_NAME = "";
  private static final short SCHEMA_VERSION = 1;

  private final String dirName;

  private final List<FileIndex> filesPresent;
  private final List<FileIndex> filesRemoved;

  // Note: subDirsPresent can also have filesRemoved and subDirsRemoved within them.
  private final List<DirIndex> subDirsPresent;
  private final List<DirIndex> subDirsRemoved;

  public DirIndex(String dirName,
      List<FileIndex> filesPresent, List<FileIndex> filesRemoved,
      List<DirIndex> subDirsPresent, List<DirIndex> subDirsRemoved) {
    Preconditions.checkNotNull(dirName); // may be empty for root dirs
    Preconditions.checkNotNull(filesPresent);
    Preconditions.checkNotNull(filesRemoved);
    Preconditions.checkNotNull(subDirsPresent);
    Preconditions.checkNotNull(subDirsRemoved);

    // validate that the same file/blob is not present in both present and removed lists
    validatePresentAndRemovedOverlap(filesPresent, filesRemoved, subDirsPresent, subDirsRemoved);

    this.dirName = dirName;
    this.filesPresent = filesPresent;
    this.filesRemoved = filesRemoved;
    this.subDirsPresent = subDirsPresent;
    this.subDirsRemoved = subDirsRemoved;
  }

  public static short getSchemaVersion() {
    return SCHEMA_VERSION;
  }

  public String getDirName() {
    return dirName;
  }

  public List<FileIndex> getFilesPresent() {
    return filesPresent;
  }

  public List<FileIndex> getFilesRemoved() {
    return filesRemoved;
  }

  public List<DirIndex> getSubDirsPresent() {
    return subDirsPresent;
  }

  public List<DirIndex> getSubDirsRemoved() {
    return subDirsRemoved;
  }

  public static Stats getStats(DirIndex dirIndex) {
    Stats stats = new Stats();
    updateStats(dirIndex, stats);
    return stats;
  }

  private static void validatePresentAndRemovedOverlap(List<FileIndex> filesPresent, List<FileIndex> filesRemoved, List<DirIndex> subDirsPresent, List<DirIndex> subDirsRemoved) {
    Set<FileIndex> filesPresentSet = new HashSet<>(filesPresent);
    Set<FileIndex> filesRemovedSet = new HashSet<>(filesRemoved);

    Set<FileBlob> blobsPresentSet = filesPresent.stream()
        .flatMap(fileIndex -> fileIndex.getBlobs().stream())
        .collect(Collectors.toCollection(HashSet::new));

    Set<FileBlob> blobsRemovedSet = filesRemoved.stream()
        .flatMap(fileIndex -> fileIndex.getBlobs().stream())
        .collect(Collectors.toCollection(HashSet::new));

    // check within current subdir, since moving file to a different subDir should be OK.
    Sets.SetView<FileIndex> presentAndRemovedFilesSet = Sets.intersection(filesPresentSet, filesRemovedSet);
    Preconditions.checkState(presentAndRemovedFilesSet.isEmpty(),
        String.format("File(s) present in both filesPresent and filesRemoved set: %s", presentAndRemovedFilesSet));

    for (DirIndex subDirPresent: subDirsPresent) {
      addFilesAndBlobsForSubDir(subDirPresent, filesPresentSet, filesRemovedSet, blobsPresentSet, blobsRemovedSet);
    }

    for (DirIndex subDirRemoved: subDirsRemoved) {
      addFilesAndBlobsForSubDirRemoved(subDirRemoved, filesRemovedSet, blobsRemovedSet);
    }

    Sets.SetView<FileBlob> presentAndRemovedBlobsSet = Sets.intersection(blobsPresentSet, blobsRemovedSet);
    Preconditions.checkState(presentAndRemovedBlobsSet.isEmpty(),
        String.format("Blob(s) present in both blobsPresentSet and blobsRemovedSet set: %s", presentAndRemovedBlobsSet));
  }

  private static void addFilesAndBlobsForSubDir(DirIndex dirIndex,
      Set<FileIndex> filesAdded, Set<FileIndex> filesRemoved,
      Set<FileBlob> blobsAdded, Set<FileBlob> blobsRemoved) {
    for (FileIndex filePresent: dirIndex.getFilesPresent()) {
      filesAdded.add(filePresent);
      blobsAdded.addAll(filePresent.getBlobs());
    }

    for (DirIndex subDir: dirIndex.getSubDirsPresent()) {
      addFilesAndBlobsForSubDir(subDir, filesAdded, filesRemoved, blobsAdded, blobsRemoved);
    }

    for (DirIndex subDir: dirIndex.getSubDirsRemoved()) {
      addFilesAndBlobsForSubDirRemoved(subDir, filesRemoved, blobsRemoved);
    }
  }

  private static void addFilesAndBlobsForSubDirRemoved(DirIndex dirIndex,
      Set<FileIndex> filesRemoved,
      Set<FileBlob> blobsRemoved) {
    for (FileIndex filePresent: dirIndex.getFilesPresent()) {
      filesRemoved.add(filePresent);
      blobsRemoved.addAll(filePresent.getBlobs());
    }

    for (FileIndex fileRemoved: dirIndex.getFilesRemoved()) {
      filesRemoved.add(fileRemoved);
      blobsRemoved.addAll(fileRemoved.getBlobs());
    }

    for (DirIndex subDir: dirIndex.getSubDirsPresent()) {
      addFilesAndBlobsForSubDirRemoved(subDir, filesRemoved, blobsRemoved);
    }
  }

  private static void updateStats(DirIndex dirIndex, Stats stats) {
    stats.filesPresent += dirIndex.getFilesPresent().size();
    stats.filesRemoved += dirIndex.getFilesRemoved().size();

    stats.subDirsPresent += dirIndex.getSubDirsPresent().size();
    stats.subDirsRemoved += dirIndex.getSubDirsRemoved().size();

    stats.bytesPresent += dirIndex.getFilesPresent().stream().mapToLong(fi -> fi.getFileMetadata().getSize()).sum();
    stats.bytesRemoved += dirIndex.getFilesRemoved().stream().mapToLong(fi -> fi.getFileMetadata().getSize()).sum();

    for (DirIndex subDirPresent : dirIndex.getSubDirsPresent()) {
      updateStats(subDirPresent, stats);
    }

    for (DirIndex subDirsRemoved : dirIndex.getSubDirsRemoved()) {
      updateStatsForSubDirsRemoved(subDirsRemoved, stats);
    }
  }

  private static void updateStatsForSubDirsRemoved(DirIndex dirIndex, Stats stats) {
    stats.filesRemoved += dirIndex.getFilesPresent().size();
    stats.bytesRemoved += dirIndex.getFilesPresent().stream().mapToLong(fi -> fi.getFileMetadata().getSize()).sum();
    for (DirIndex subDirToRemove : dirIndex.getSubDirsPresent()) {
      updateStatsForSubDirsRemoved(subDirToRemove, stats);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    DirIndex that = (DirIndex) o;

    return new EqualsBuilder()
        .append(getDirName(), that.getDirName())
        .append(getFilesPresent(), that.getFilesPresent())
        .append(getFilesRemoved(), that.getFilesRemoved())
        .append(getSubDirsPresent(), that.getSubDirsPresent())
        .append(getSubDirsRemoved(), that.getSubDirsRemoved())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(getDirName()).append(getFilesPresent())
        .append(getFilesRemoved())
        .append(getSubDirsPresent())
        .append(getSubDirsRemoved())
        .toHashCode();
  }

  @Override
  public String toString() {
    return "DirIndex{" +
        "dirName='" +
        dirName + '\'' +
        ", filesPresent=" + filesPresent +
        ", filesRemoved=" + filesRemoved +
        ", subDirsPresent=" + subDirsPresent +
        ", subDirsRemoved=" + subDirsRemoved +
        '}';
  }

  public static class Stats {
    public int filesPresent;
    public int filesRemoved;

    public int subDirsPresent;
    public int subDirsRemoved;

    public long bytesPresent;
    public long bytesRemoved;

    @Override
    public String toString() {
      return "Stats{" +
          "filesAdded=" + filesPresent +
          ", filesRemoved=" + filesRemoved +
          ", subDirsAdded=" + subDirsPresent +
          ", subDirsRemoved=" + subDirsRemoved +
          ", bytesAdded=" + bytesPresent +
          ", bytesRemoved=" + bytesRemoved +
          '}';
    }
  }
}
