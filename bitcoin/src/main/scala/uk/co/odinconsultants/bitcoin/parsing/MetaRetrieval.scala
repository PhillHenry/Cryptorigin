package uk.co.odinconsultants.bitcoin.parsing

import uk.co.odinconsultants.bitcoin.parsing.Indexer._

trait MetaRetrieval extends (BackReference => PubKey)
