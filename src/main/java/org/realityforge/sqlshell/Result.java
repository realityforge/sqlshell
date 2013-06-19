package org.realityforge.sqlshell;

import java.util.List;
import java.util.Map;

final class Result
{
  private final List<Map<String, Object>> _rows;
  private final int _updateCount;

  Result( final List<Map<String, Object>> rows, final int updateCount )
  {
    _rows = rows;
    _updateCount = updateCount;
  }

  boolean isQuery()
  {
    return null != _rows;
  }

  List<Map<String, Object>> getRows()
  {
    return _rows;
  }

  int getUpdateCount()
  {
    return _updateCount;
  }
}
