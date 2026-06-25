// Package eksauth builds a rest.Config for an EKS cluster using IAM
// authentication (Pod Identity / IRSA). It calls DescribeCluster to discover
// the API endpoint and CA, then generates presigned STS tokens as bearer
// credentials — the same mechanism as `aws eks get-token`.
package eksauth

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	smithymiddleware "github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"k8s.io/client-go/rest"
)

const (
	tokenPrefix     = "k8s-aws-v1."
	tokenExpiry     = 14 * time.Minute
	clusterIDHeader = "x-k8s-aws-id"
)

// NewRESTConfig returns a rest.Config that authenticates to the given EKS
// cluster using IAM credentials from the ambient environment (Pod Identity,
// IRSA, instance profile, etc.).
func NewRESTConfig(ctx context.Context, awsCfg aws.Config, clusterName string) (*rest.Config, error) {
	eksClient := eks.NewFromConfig(awsCfg)
	out, err := eksClient.DescribeCluster(ctx, &eks.DescribeClusterInput{
		Name: &clusterName,
	})
	if err != nil {
		return nil, fmt.Errorf("describe cluster %s: %w", clusterName, err)
	}

	ca, err := base64.StdEncoding.DecodeString(*out.Cluster.CertificateAuthority.Data)
	if err != nil {
		return nil, fmt.Errorf("decode CA: %w", err)
	}

	provider := &tokenProvider{
		sts: sts.NewFromConfig(awsCfg, func(o *sts.Options) {
			o.BaseEndpoint = aws.String("https://sts." + awsCfg.Region + ".amazonaws.com")
		}),
		clusterName: clusterName,
	}

	return &rest.Config{
		Host: *out.Cluster.Endpoint,
		TLSClientConfig: rest.TLSClientConfig{
			CAData: ca,
		},
		WrapTransport: func(rt http.RoundTripper) http.RoundTripper {
			return &tokenRoundTripper{delegate: rt, provider: provider}
		},
	}, nil
}

type tokenProvider struct {
	sts         *sts.Client
	clusterName string

	mu     sync.Mutex
	token  string
	expiry time.Time
}

func (p *tokenProvider) Token(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.token != "" && time.Now().Before(p.expiry.Add(-1*time.Minute)) {
		return p.token, nil
	}

	presignClient := sts.NewPresignClient(p.sts)
	presigned, err := presignClient.PresignGetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}, func(o *sts.PresignOptions) {
		o.ClientOptions = append(o.ClientOptions, func(opts *sts.Options) {
			opts.APIOptions = append(opts.APIOptions, func(stack *smithymiddleware.Stack) error {
				return stack.Build.Add(&addHeaderMiddleware{
					key:   clusterIDHeader,
					value: p.clusterName,
				}, smithymiddleware.After)
			})
		})
	})
	if err != nil {
		return "", fmt.Errorf("presign GetCallerIdentity: %w", err)
	}

	p.token = tokenPrefix + base64.RawURLEncoding.EncodeToString([]byte(presigned.URL))
	p.expiry = time.Now().Add(tokenExpiry)
	return p.token, nil
}

// addHeaderMiddleware injects a header into the HTTP request before signing.
type addHeaderMiddleware struct {
	key, value string
}

func (m *addHeaderMiddleware) ID() string { return "AddClusterIDHeader" }

func (m *addHeaderMiddleware) HandleBuild(
	ctx context.Context,
	in smithymiddleware.BuildInput,
	next smithymiddleware.BuildHandler,
) (smithymiddleware.BuildOutput, smithymiddleware.Metadata, error) {
	if req, ok := in.Request.(*smithyhttp.Request); ok {
		req.Header.Set(m.key, m.value)
	}
	return next.HandleBuild(ctx, in)
}

type tokenRoundTripper struct {
	delegate http.RoundTripper
	provider *tokenProvider
}

func (t *tokenRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := t.provider.Token(req.Context())
	if err != nil {
		return nil, err
	}
	clone := req.Clone(req.Context())
	clone.Header.Set("Authorization", "Bearer "+token)
	return t.delegate.RoundTrip(clone)
}
