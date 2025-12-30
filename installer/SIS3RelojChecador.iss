#define MyAppName "SIS3RelojChecador"
#define MyAppVersion "1.0.0"
#define MyAppPublisher "MACASA"
#define MyAppExeName "SIS3RelojChecador.exe"

[Setup]
AppId={{D2D4B2B1-8E6F-4E2E-9D1F-REPLACE-ME-UNIQUE}}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
DisableProgramGroupPage=yes
OutputDir=output
OutputBaseFilename=setup_{#MyAppName}
Compression=lzma
SolidCompression=yes

; Requerimos elevación porque instalaremos ODBC si falta
PrivilegesRequired=admin

; Windows 11 Pro ok
ArchitecturesAllowed=x64
ArchitecturesInstallIn64BitMode=x64

[Languages]
Name: "spanish"; MessagesFile: "compiler:Languages\Spanish.isl"

[Tasks]
Name: "desktopicon"; Description: "Crear acceso directo en el escritorio"; GroupDescription: "Accesos directos:"; Flags: unchecked

[Files]
; Copia TODO lo de PyInstaller onedir
Source: "..\dist\SIS3RelojChecador\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

; Copia config.ini inicial a AppData solo si no existe
Source: "..\config.example.ini"; DestDir: "{localappdata}\{#MyAppName}"; DestName: "config.ini"; Flags: onlyifdoesntexist

[Icons]
Name: "{autoprograms}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
; Instalar ODBC Driver 18 si no está presente.
; El MSI se empaqueta en {app}\assets\odbc\msodbcsql18.msi
Filename: "msiexec.exe"; Parameters: "/i ""{app}\assets\odbc\msodbcsql18.msi"" /quiet /norestart IACCEPTMSODBCSQLLICENSETERMS=YES"; \
  StatusMsg: "Instalando Microsoft ODBC Driver 18 for SQL Server..."; Flags: runhidden waituntilterminated; Check: NeedsODBC18

; Ejecutar app al terminar (opcional)
Filename: "{app}\{#MyAppExeName}"; Description: "Abrir {#MyAppName}"; Flags: nowait postinstall skipifsilent

[Code]
function ODBCDriverExists(DriverName: String): Boolean;
var
  RootKey: Integer;
  I: Integer;
  KeyName: String;
begin
  Result := False;

  // 64-bit ODBC drivers
  RootKey := HKLM64;
  for I := 0 to 200 do
  begin
    KeyName := 'SOFTWARE\ODBC\ODBCINST.INI\' + DriverName;
    if RegKeyExists(RootKey, KeyName) then
    begin
      Result := True;
      Exit;
    end;
    Break; // no loop needed; kept structure for readability
  end;

  // fallback 32-bit
  RootKey := HKLM32;
  KeyName := 'SOFTWARE\ODBC\ODBCINST.INI\' + DriverName;
  if RegKeyExists(RootKey, KeyName) then
  begin
    Result := True;
    Exit;
  end;
end;

function NeedsODBC18(): Boolean;
begin
  // Si existe 18, no instalar.
  // Si no existe 18, sí instalar.
  Result := not ODBCDriverExists('ODBC Driver 18 for SQL Server');
end;
