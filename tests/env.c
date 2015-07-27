#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

//TODO for shell env
int main(int argc, char **argv)
{
  putenv("SomeVariable=SomeValue");
  return 0;
}
