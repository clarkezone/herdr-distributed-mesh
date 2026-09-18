//go:build windows

package meshlocal

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
	"unsafe"

	"github.com/tailscale/go-winio"
	"golang.org/x/sys/windows"
)

func samePath(a, b string) bool { return strings.EqualFold(a, b) }

func replaceStatus(from, to string) error {
	// Windows metadata readers and scanners can briefly deny replacement even
	// though managed readers share delete access. Never truncate the old file.
	for attempt := 0; ; attempt++ {
		err := os.Rename(from, to)
		if attempt >= 50 || (!errors.Is(err, windows.ERROR_ACCESS_DENIED) && !errors.Is(err, windows.ERROR_SHARING_VIOLATION)) {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func openPrivateRead(path string) (*os.File, error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	handle, err := windows.CreateFile(name, windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return nil, err
	}
	if err := verifyPrivateFile(handle); err != nil {
		return nil, errors.Join(err, windows.CloseHandle(handle))
	}
	return os.NewFile(uintptr(handle), path), nil
}

func verifyPrivateFile(handle windows.Handle) error {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		return err
	}
	if info.NumberOfLinks != 1 || info.FileAttributes&(windows.FILE_ATTRIBUTE_REPARSE_POINT|windows.FILE_ATTRIBUTE_DIRECTORY) != 0 {
		return errors.New("managed state file is a link or directory")
	}
	sd, err := windows.GetSecurityInfo(handle, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return err
	}
	dacl, _, err := sd.DACL()
	if err != nil {
		return err
	}
	if dacl == nil || dacl.AceCount == 0 {
		return errors.New("managed state file has no private access policy")
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return err
	}
	system, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	if err != nil {
		return err
	}
	for i := uint32(0); i < uint32(dacl.AceCount); i++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, i, &ace); err != nil {
			return err
		}
		if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
			return errors.New("managed state file has an unsupported access policy")
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		if !sid.Equals(user.User.Sid) && !sid.Equals(system) {
			return errors.New("managed state file is accessible to another principal")
		}
	}
	return nil
}

func pipeDetails(dir string) (string, string, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return "", "", err
	}
	sid := user.User.Sid.String()
	hash := sha256.Sum256([]byte(sid + "\x00" + strings.ToLower(dir)))
	return fmt.Sprintf(`\\.\pipe\herdr-mesh-%x`, hash[:16]), "D:P(A;;GA;;;" + sid + ")(A;;GA;;;SY)", nil
}

func listenIPC(dir string) (net.Listener, error) {
	path, acl, err := pipeDetails(dir)
	if err != nil {
		return nil, err
	}
	return winio.ListenPipe(path, &winio.PipeConfig{SecurityDescriptor: acl, InputBufferSize: 65536, OutputBufferSize: 65536})
}

func dialIPC(ctx context.Context, dir string) (net.Conn, error) {
	path, _, err := pipeDetails(dir)
	if err != nil {
		return nil, err
	}
	connection, err := winio.DialPipeContext(ctx, path)
	if err != nil {
		return nil, err
	}
	if err := verifyPipeServer(connection); err != nil {
		return nil, errors.Join(err, connection.Close())
	}
	return connection, nil
}

func verifyPipeServer(connection net.Conn) error {
	handle, ok := connection.(interface{ Fd() uintptr })
	if !ok {
		return errors.New("managed pipe has no verifiable native handle")
	}
	var pid uint32
	if err := windows.GetNamedPipeServerProcessId(windows.Handle(handle.Fd()), &pid); err != nil {
		return err
	}
	process, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, pid)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(process)
	var token windows.Token
	if err := windows.OpenProcessToken(process, windows.TOKEN_QUERY, &token); err != nil {
		return err
	}
	defer token.Close()
	owner, err := token.GetTokenUser()
	if err != nil {
		return err
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return err
	}
	system, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	if err != nil {
		return err
	}
	if !owner.User.Sid.Equals(user.User.Sid) && !owner.User.Sid.Equals(system) {
		return errors.New("managed pipe server belongs to another user")
	}
	return nil
}
