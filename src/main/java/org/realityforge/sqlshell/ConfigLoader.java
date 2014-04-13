package org.realityforge.sqlshell;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.sql.Driver;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.realityforge.getopt4j.CLArgsParser;
import org.realityforge.getopt4j.CLOption;
import org.realityforge.getopt4j.CLOptionDescriptor;
import org.realityforge.getopt4j.CLUtil;
import org.realityforge.sqlshell.data_type.mssql.ServerConfig;
import org.realityforge.sqlshell.mssql.Runner;

/**
 * The entry point in which applies a configuration to a database server.
 */
public class ConfigLoader
{
  private static final int HELP_OPT = 1;
  private static final int QUIET_OPT = 'q';
  private static final int VERBOSE_OPT = 'v';
  private static final int DATABASE_DRIVER_OPT = 2;
  private static final int DATABASE_PROPERTY_OPT = 'D';
  private static final int DATABASE_DIALECT_OPT = 'd';
  private static final int FILE_OPT = 'f';

  private static final CLOptionDescriptor[] OPTIONS = new CLOptionDescriptor[]{
    new CLOptionDescriptor( "database-driver",
                            CLOptionDescriptor.ARGUMENT_REQUIRED,
                            DATABASE_DRIVER_OPT,
                            "The jdbc driver to load prior to connecting to the databases." ),
    new CLOptionDescriptor( "database-property",
                            CLOptionDescriptor.ARGUMENTS_REQUIRED_2 | CLOptionDescriptor.DUPLICATES_ALLOWED,
                            DATABASE_PROPERTY_OPT,
                            "A jdbc property." ),
    new CLOptionDescriptor( "dialect",
                            CLOptionDescriptor.ARGUMENT_REQUIRED,
                            DATABASE_DIALECT_OPT,
                            "Database dialect.  Currently only 'mssql' supported" ),
    new CLOptionDescriptor( "file",
                            CLOptionDescriptor.ARGUMENT_REQUIRED,
                            FILE_OPT,
                            "A file containing a JSON configuration for the database." ),
    new CLOptionDescriptor( "help",
                            CLOptionDescriptor.ARGUMENT_DISALLOWED,
                            HELP_OPT,
                            "print this message and exit" ),
    new CLOptionDescriptor( "quiet",
                            CLOptionDescriptor.ARGUMENT_DISALLOWED,
                            QUIET_OPT,
                            "Do not output unless an error occurs, just return 0 on no difference.",
                            new int[]{ VERBOSE_OPT } ),
    new CLOptionDescriptor( "verbose",
                            CLOptionDescriptor.ARGUMENT_DISALLOWED,
                            VERBOSE_OPT,
                            "Verbose output of differences.",
                            new int[]{ QUIET_OPT } ),
  };

  private static final int ERROR_PARSING_ARGS_EXIT_CODE = 2;
  private static final int ERROR_BAD_DRIVER_EXIT_CODE = 3;
  private static final int ERROR_OTHER_EXIT_CODE = 4;

  private static String c_databaseDriver;
  private static final SqlShell c_shell = new SqlShell();
  private static final Logger c_logger = Logger.getAnonymousLogger();
  private static String c_inputFile;
  private static String c_dialect;

  public static void main( final String[] args )
  {
    setupLogger();
    if ( !processOptions( args ) )
    {
      System.exit( ERROR_PARSING_ARGS_EXIT_CODE );
      return;
    }

    if ( c_logger.isLoggable( Level.FINE ) )
    {
      c_logger.log( Level.FINE, "SqlShell starting..." );
    }

    final Driver driver = loadDatabaseDriver();
    if ( null == driver )
    {
      System.exit( ERROR_BAD_DRIVER_EXIT_CODE );
      return;
    }

    c_shell.setDriver( driver );

    final ObjectMapper mapper = new ObjectMapper();
    try
    {
      (new Runner(c_shell)).apply( mapper.readValue( new File(c_inputFile) , ServerConfig.class ) );
    }
    catch ( final IOException e )
    {
      c_logger.log( Level.SEVERE, "Error: Unable to load input file " + c_inputFile + " due to: " + e, e );
      System.exit( ERROR_OTHER_EXIT_CODE );
      return;
    }
    catch ( final Exception e )
    {
      c_logger.log( Level.SEVERE, "Error: Unable to apply changes to database from file " + c_inputFile + " due to: " + e, e );
      e.printStackTrace();
      System.exit( ERROR_OTHER_EXIT_CODE );
      return;
    }

    if ( c_logger.isLoggable( Level.FINE ) )
    {
      c_logger.log( Level.FINE, "SqlShell completed." );
    }
  }

  private static void setupLogger()
  {
    c_logger.setUseParentHandlers( false );
    final StdOutHandler handler = new StdOutHandler();
    handler.setLevel( Level.ALL );
    c_logger.addHandler( handler );
  }

  private static Driver loadDatabaseDriver()
  {
    try
    {
      return (Driver) Class.forName( c_databaseDriver ).newInstance();
    }
    catch ( final Exception e )
    {
      c_logger.log( Level.SEVERE, "Error: Unable to load database driver " + c_databaseDriver + " due to " + e );
      System.exit( ERROR_BAD_DRIVER_EXIT_CODE );
      return null;
    }
  }

  private static boolean processOptions( final String[] args )
  {
    // Parse the arguments
    final CLArgsParser parser = new CLArgsParser( args, OPTIONS );

    // Make sure that there was no errors parsing arguments
    if ( null != parser.getErrorString() )
    {
      c_logger.log( Level.SEVERE, "Error: " + parser.getErrorString() );
      printUsage();
      return false;
    }

    // Get a list of parsed options
    @SuppressWarnings( "unchecked" ) final List<CLOption> options = parser.getArguments();
    for ( final CLOption option : options )
    {
      switch ( option.getId() )
      {
        case CLOption.TEXT_ARGUMENT:
        {
          if ( null == c_shell.getDatabase() )
          {
            c_shell.setDatabase( option.getArgument() );
          }
          else
          {
            c_logger.log( Level.SEVERE, "Error: Unexpected argument: " + option.getArgument() );
            return false;
          }
          break;
        }
        case DATABASE_PROPERTY_OPT:
        {
          c_shell.getDbProperties().setProperty( option.getArgument(), option.getArgument( 1 ) );
          break;
        }
        case DATABASE_DRIVER_OPT:
        {
          c_databaseDriver = option.getArgument();
          break;
        }
        case FILE_OPT:
        {
          c_inputFile = option.getArgument();
          break;
        }
        case DATABASE_DIALECT_OPT:
        {
          c_dialect = option.getArgument();
          if ( !"mssql".equals( c_dialect ) )
          {
            c_logger.log( Level.SEVERE, "Unsupported database dialect: " + c_dialect );
            c_logger.log( Level.SEVERE, "Supported database dialects are: mssql" );
            return false;
          }
          break;
        }
        case VERBOSE_OPT:
        {
          c_logger.setLevel( Level.ALL );
          break;
        }
        case QUIET_OPT:
        {
          c_logger.setLevel( Level.WARNING );
          break;
        }
        case HELP_OPT:
        {
          printUsage();
          return false;
        }
      }
    }
    if ( null == c_databaseDriver )
    {
      c_logger.log( Level.SEVERE, "Error: Database driver must be specified" );
      return false;
    }
    if ( null == c_shell.getDatabase() )
    {
      c_logger.log( Level.SEVERE, "Error: Jdbc url must supplied for the database" );
      return false;
    }
    if ( null == c_dialect )
    {
      c_logger.log( Level.SEVERE, "Error: Database dialect must specified" );
      return false;
    }
    if ( null == c_inputFile )
    {
      c_logger.log( Level.SEVERE, "Error: Input JSON file must specified" );
      return false;
    }
    if ( c_logger.isLoggable( Level.FINE ) )
    {
      c_logger.log( Level.FINE, "Database: " + c_shell.getDatabase() );
      c_logger.log( Level.FINE, "Database Driver: " + c_databaseDriver );
      c_logger.log( Level.FINE, "Database Properties: " + c_shell.getDbProperties() );
      if ( null != c_inputFile )
      {
        c_logger.log( Level.FINE, "Input File: " + c_inputFile );
      }
    }

    return true;
  }

  /**
   * Print out a usage statement
   */
  private static void printUsage()
  {
    final String lineSeparator = System.getProperty( "line.separator" );

    c_logger.log( Level.INFO, "java " +
                              ConfigLoader.class.getName() +
                              " [options] jdbcURL" +
                              lineSeparator +
                              "Options: " +
                              lineSeparator +
                              CLUtil.describeOptions( OPTIONS ).toString() );
  }
}
