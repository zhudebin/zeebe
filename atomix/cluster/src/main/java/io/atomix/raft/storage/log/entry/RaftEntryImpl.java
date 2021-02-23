package io.atomix.raft.storage.log.entry;

import io.atomix.raft.storage.RaftFrameReader;
import io.atomix.raft.storage.impl.ConfigurationEntryDecoder;
import io.atomix.raft.storage.impl.InitialEntryDecoder;
import io.atomix.raft.storage.impl.MessageHeaderDecoder;
import io.atomix.raft.storage.impl.ZeebeEntryDecoder;
import io.zeebe.journal.JournalRecord;
import org.agrona.DirectBuffer;

public class RaftEntryImpl implements RaftEntry {

  private final RaftFrameReader reader;
  private final JournalRecord record;

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

  public RaftEntryImpl(final RaftFrameReader reader, final JournalRecord record) {
    this.reader = reader;
    this.record = record;
  }

  @Override
  public long term() {
    return reader.term();
  }

  @Override
  public boolean isApplicationEntry() {
    headerDecoder.wrap(entry(), 0);
    return headerDecoder.schemaId() == ZeebeEntryDecoder.SCHEMA_ID
        && headerDecoder.templateId() == ZeebeEntryDecoder.TEMPLATE_ID;
  }

  @Override
  public ApplicationEntryImpl asApplicationEntry() {
    return new ApplicationEntryImpl(this);
  }

  @Override
  public boolean isConfigurationEntry() {
    headerDecoder.wrap(entry(), 0);
    return headerDecoder.schemaId() == ConfigurationEntryDecoder.SCHEMA_ID
        && headerDecoder.templateId() == ConfigurationEntryDecoder.TEMPLATE_ID;
  }

  @Override
  public ConfigurationEntry asConfigurationEntry() {
    return null;
  }

  @Override
  public boolean isInitialEntry() {
    headerDecoder.wrap(entry(), 0);
    return headerDecoder.schemaId() == InitialEntryDecoder.SCHEMA_ID
        && headerDecoder.templateId() == InitialEntryDecoder.TEMPLATE_ID;
  }

  @Override
  public InitializeEntry asInitialEntry() {
    return null;
  }

  @Override
  public long index() {
    return record.index();
  }

  @Override
  public long asqn() {
    return record.asqn();
  }

  @Override
  public int checksum() {
    return record.checksum();
  }

  @Override
  public DirectBuffer data() {
    return record.data();
  }

  @Override
  public DirectBuffer entry() {
    return reader.data();
  }

  @Override
  public int size() {
    // TODO: add length to journal record and return here
    return reader.getLength();
  }
}
