package akka.persistence.cassandra.snapshot

import com.typesafe.config.Config

import akka.persistence.cassandra.CassandraPluginConfig

class CassandraSnapshotStoreConfig(config: Config) extends CassandraPluginConfig(config) {
  val maxMetadataResultSize = config.getInt("max-metadata-result-size")
  val gc_grace_seconds: Long = config.getLong("gc-grace-seconds")
  val compaction_strategy: String = config.getString("compaction_strategy")
}
