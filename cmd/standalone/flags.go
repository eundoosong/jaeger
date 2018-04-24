// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"

	"github.com/spf13/viper"
)

const (
	HealthCheckHTTPPort = "standalone.health-check-http-port"
)

// QueryOptions holds configuration for query service
type StandaloneOptions struct {
	HealthCheckHTTPPort int
}

// AddFlags adds flags for QueryOptions
func AddFlags(flagSet *flag.FlagSet) {
	flagSet.Int(HealthCheckHTTPPort, 16687, "The http port for the health check service")
}

// InitFromViper initializes QueryOptions with properties from viper
func (sOpts *StandaloneOptions) InitFromViper(v *viper.Viper) *QueryOptions {
	sOpts.HealthCheckHTTPPort = v.GetInt(HealthCheckHTTPPort)
	return sOpts
}