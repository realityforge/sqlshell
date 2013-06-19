package org.realityforge.sqlshell;

import java.util.logging.Handler;
import java.util.logging.LogRecord;

final class StdOutHandler
  extends Handler
{
  @Override
  public void publish( final LogRecord logRecord )
  {
    System.out.println( logRecord.getMessage() );
    flush();
  }

  @Override
  public void flush()
  {
    System.out.flush();
  }

  @Override
  public void close()
    throws SecurityException
  {
    flush();
  }
}
