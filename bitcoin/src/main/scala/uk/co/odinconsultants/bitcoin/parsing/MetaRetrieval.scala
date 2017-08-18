package uk.co.odinconsultants.bitcoin.parsing

import uk.co.odinconsultants.bitcoin.parsing.Indexer._

trait MetaRetrieval extends (List[BackReference] => List[PubKey])
