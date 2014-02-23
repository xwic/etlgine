@ECHO OFF
JAVA -server -cp "./lib/*" -Xmx768m de.xwic.etlgine.server.Launch %1 %2 %3 %4 %5 %6 %7 %8 %9
goto end
:err
ECHO ERROR!
ECHO .
ECHO You must run the run_XXXX batch files.

:end
pause