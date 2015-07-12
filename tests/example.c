#include "SkipDB.h"

int main(void)
{
	Datum key;
	Datum value;
	Datum key2;
	Datum value2;
	int count;

	// open
	
	SkipDB *db = SkipDB_new();
	SkipDB_setPath_(db, "test.skipdb");
	SkipDB_open(db);
	
	// write
	
	SkipDB_beginTransaction(db);
	key = Datum_FromCString_("testKey");
	value = Datum_FromCString_("testValue");
	SkipDB_at_put_(db, key, value);

	key2 = Datum_FromCString_("testKey2");
	value2 = Datum_FromCString_("testValue2");
	SkipDB_at_put_(db, key2, value2);

	SkipDB_commitTransaction(db);
        SkipDB_show(db);
	
	// read
	
	value = SkipDB_at_(db, key2);
        printf("%s=%s\n", key2.data, value2.data);
	
	// count
	
	count = SkipDB_count(db);

	// remove
	
	SkipDB_beginTransaction(db);
	SkipDB_removeAt_(db, key);
	SkipDB_commitTransaction(db);
	
	// there's also a cursor API
	// not shown in this example code

	// close
	
	SkipDB_close(db);
	
	return 0;
}
