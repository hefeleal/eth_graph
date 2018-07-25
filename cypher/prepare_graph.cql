CREATE CONSTRAINT ON ( address:Address ) ASSERT address.hash IS UNIQUE;
CREATE CONSTRAINT ON ( block:Block ) ASSERT block.hash IS UNIQUE;
CREATE CONSTRAINT ON ( transaction:Transaction ) ASSERT transaction.hash IS UNIQUE;
