package ironflock

import (
	"fmt"
	"os"
	"strings"

	"github.com/RecordEvolution/ironflock-go/internal/env"
)

// defaultTunnelDomain is the public tunnel edge of the IronFlock cloud.
const defaultTunnelDomain = "app.ironflock.com"

// maxDNSLabelLength is the longest single DNS label (RFC 1035), which a
// tunnel label must fit in.
const maxDNSLabelLength = 63

// GetRemoteAccessURLForPort returns the public URL of a port declared in
// the app's port-template.yml, once its tunnel is active. protocol is
// "http" (default when empty), "https", "tcp" or "udp"; tcp/udp ports need
// the template's remote_port_environment name. Port values are read live
// from /data/env, so call it again rather than caching the result. It
// returns false when the URL cannot be composed.
//
// http and https ports are served under the tunnel label
// <DEVICE_KEY>-<app name, lower case>-<port> (https ports with the
// platform's "secure-" prefix) on the tunnel domain the device agent injects
// (TUNNEL_DOMAIN), always as an https:// URL. On a device that belongs to an
// instance (appliance, INSTANCE_KEY set) the cloud-forwarded route
// https://i<INSTANCE_KEY>-<label>.<CLOUD_TUNNEL_DOMAIN> is returned instead.
// The label must be a single valid DNS label: at most 63 characters, no dot
// (an app name containing a dot cannot be tunneled).
//
// tcp and udp ports use the tunnel-assigned public port the platform injects
// under remotePortEnvironment: <protocol>://<TUNNEL_DOMAIN>:<port>. On an
// instance device the internet-facing port arrives as
// <remotePortEnvironment>_CLOUD and is preferred
// (<protocol>://<CLOUD_TUNNEL_DOMAIN>:<port>); until it appears, the
// instance-local URL is returned.
//
// Unset or empty tunnel domains default to app.ironflock.com.
func (f *IronFlock) GetRemoteAccessURLForPort(port int, protocol, remotePortEnvironment string) (string, bool) {
	protocol = strings.ToLower(strings.TrimSpace(protocol))
	if protocol == "" {
		protocol = "http"
	}
	instanceKey := os.Getenv("INSTANCE_KEY")

	switch protocol {
	case "tcp", "udp":
		if remotePortEnvironment == "" {
			return "", false
		}
		if instanceKey != "" {
			if cloudPort := env.ReadInjected(remotePortEnvironment + "_CLOUD"); cloudPort != "" {
				return fmt.Sprintf("%s://%s:%s", protocol, tunnelDomain("CLOUD_TUNNEL_DOMAIN"), cloudPort), true
			}
		}
		remotePort := env.ReadInjected(remotePortEnvironment)
		if remotePort == "" {
			return "", false
		}
		return fmt.Sprintf("%s://%s:%s", protocol, tunnelDomain("TUNNEL_DOMAIN"), remotePort), true
	case "http", "https":
	default:
		return "", false
	}

	if f.deviceKey == "" || f.appName == "" {
		return "", false
	}
	label := fmt.Sprintf("%s-%s-%d", f.deviceKey, strings.ToLower(f.appName), port)
	if protocol == "https" {
		// https-protocol tunnels get secure- prefixed subdomains platform-wide.
		label = "secure-" + label
	}
	domain := tunnelDomain("TUNNEL_DOMAIN")
	if instanceKey != "" {
		// Instance route: forwarded through the cloud tunnel edge.
		label = "i" + instanceKey + "-" + label
		domain = tunnelDomain("CLOUD_TUNNEL_DOMAIN")
	}
	if len(label) > maxDNSLabelLength || strings.Contains(label, ".") {
		return "", false
	}
	return fmt.Sprintf("https://%s.%s", label, domain), true
}

// tunnelDomain returns the tunnel domain the environment variable name holds,
// or the public cloud edge when it is unset or empty.
func tunnelDomain(name string) string {
	if d := os.Getenv(name); d != "" {
		return d
	}
	return defaultTunnelDomain
}
