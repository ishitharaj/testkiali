package zipkin

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/kiali/kiali/config"
	"github.com/kiali/kiali/log"
	"github.com/kiali/kiali/models"
	"github.com/kiali/kiali/tracing/jaeger/model"
	"github.com/kiali/kiali/util"
)

type ZipkinHTTPClient struct {
	IgnoreCluster bool
}

// New client
func NewZipkinClient(client http.Client, baseURL *url.URL) (zipkinClient *ZipkinHTTPClient, err error) {
	url := *baseURL
	conf := config.Get()
	var ignoreCluster bool
	var zipkinService string
	services := model.Services{}

	url.Path = path.Join(url.Path, "/api/v2/services")
	resp, code, reqError := makeRequest(client, url.String(), nil)
	if code != 200 || reqError != nil {
		log.Debugf("Error getting query for tracing. cluster tags will be disabled.")
		ignoreCluster = true
		return &ZipkinHTTPClient{IgnoreCluster: ignoreCluster}, nil
	}
	errUnmarshall := json.Unmarshal(resp, &services)
	if errUnmarshall != nil {
		log.Debugf("Error getting query for tracing. cluster tags will be disabled.")
		ignoreCluster = true
		return &ZipkinHTTPClient{IgnoreCluster: ignoreCluster}, nil
	}
	for _, service := range services.Data {
		if !strings.Contains(service, "istio") && !strings.Contains(service, "jaeger") {
			zipkinService = service
			break
		}
	}
	urlTraces := *baseURL
	urlTraces.Path = path.Join(urlTraces.Path, "/api/v2/trace")

	// if cluster exists in tags, use it
	query := models.TracingQuery{}
	tags := map[string]string{
		"cluster": conf.KubernetesConfig.ClusterName,
	}
	query.Tags = tags
	query.End = time.Now()
	query.Start = query.End.Add(-10 * time.Minute)
	query.Limit = 100
	prepareQuery(&urlTraces, zipkinService, query, false)
	r, err := queryTracesHTTP(client, &urlTraces)

	if r != nil && err == nil && len(r.Data) == 0 || err != nil {
		log.Debugf("Error getting query for tracing. cluster tags will be disabled.")
		ignoreCluster = true
	} else {
		ignoreCluster = false
	}

	return &ZipkinHTTPClient{IgnoreCluster: ignoreCluster}, nil
}

func (zc ZipkinHTTPClient) GetAppTracesHTTP(client http.Client, baseURL *url.URL, serviceName string, q models.TracingQuery) (response *model.TracingResponse, err error) {
	url := *baseURL
	url.Path = path.Join(url.Path, "/api/v2/trace")

	// if cluster exists in tags, use it
	prepareQuery(&url, serviceName, q, zc.IgnoreCluster)
	r, err := queryTracesHTTP(client, &url)

	if r != nil {
		r.TracingServiceName = serviceName
		if zc.IgnoreCluster {
			r.FromAllClusters = true
		}

	}

	return r, err
}

func (zc ZipkinHTTPClient) GetTraceDetailHTTP(client http.Client, endpoint *url.URL, traceID string) (*model.TracingSingleTrace, error) {
	u := *endpoint
	// /zipkin/api/v2/trace/<traceid>?mode=xxxx&blockStart=0000&blockEnd=FFFF&start=<start>&end=<end>
	u.Path = path.Join(u.Path, "/api/v2/trace/"+traceID)
	resp, code, reqError := makeRequest(client, u.String(), nil)
	if reqError != nil {
		log.Errorf("Zipkin query error: %s [code: %d, URL: %v]", reqError, code, u)
		return nil, reqError
	}
	// Zipkin would return "200 OK" when trace is not found, with an empty response
	if len(resp) == 0 {
		return nil, nil
	}
	response, err := convertZipkinToJaeger(resp)
	if err != nil {
		return nil, err
	}
	if len(response.Data) == 0 {
		return &model.TracingSingleTrace{Errors: response.Errors}, nil
	}
	return &model.TracingSingleTrace{
		Data:   response.Data[0],
		Errors: response.Errors,
	}, nil
}

func (zc ZipkinHTTPClient) GetServiceStatusHTTP(client http.Client, baseURL *url.URL) (bool, error) {
	url := *baseURL
	url.Path = path.Join(url.Path, "/api/v2/services")
	_, _, reqError := makeRequest(client, url.String(), nil)
	return reqError == nil, reqError
}

func queryTracesHTTP(client http.Client, u *url.URL) (*model.TracingResponse, error) {
	// HTTP and GRPC requests co-exist, but when minDuration is present, for HTTP it requires a unit (us)
	// https://github.com/kiali/kiali/issues/3939
	minDuration := u.Query().Get("minDuration")
	if minDuration != "" && !strings.HasSuffix(minDuration, "us") {
		query := u.Query()
		query.Set("minDuration", minDuration+"us")
		u.RawQuery = query.Encode()
	}
	resp, code, reqError := makeRequest(client, u.String(), nil)
	if reqError != nil {
		log.Errorf("Zipkin query error: %s [code: %d, URL: %v]", reqError, code, u)
		return &model.TracingResponse{}, reqError
	}
	return convertZipkinToJaeger(resp)
}

func unmarshal(r []byte, u *url.URL) (*model.TracingResponse, error) {
	var response model.TracingResponse
	if errMarshal := json.Unmarshal(r, &response); errMarshal != nil {
		log.Errorf("Error unmarshalling Zipkin response: %s [URL: %v]", errMarshal, u)
		return nil, errMarshal
	}
	return &response, nil
}

func convertZipkinToJaeger(zipkinResponse []byte) (*model.TracingResponse, error) {
	// Convert Zipkin response to Jaeger model here
	// ...

	return nil, nil
}

func prepareQuery(u *url.URL, zipkinServiceName string, query models.TracingQuery, ignoreCluster bool) {
	q := url.Values{}
	q.Set("serviceName", zipkinServiceName)
	q.Set("start", fmt.Sprintf("%d", query.Start.Unix()*time.Second.Microseconds()))
	q.Set("end", fmt.Sprintf("%d", query.End.Unix()*time.Second.Microseconds()))
	var tags = util.CopyStringMap(query.Tags)

	if ignoreCluster {
		delete(tags, "cluster")
	}
	if len(tags) > 0 {
		// Tags must be json encoded
		tagsJson, err := json.Marshal(tags)
		if err != nil {
			log.Errorf("Zipkin query: error while marshalling tags to json: %v", err)
		}
		q.Set("tags", string(tagsJson))
	}
	if query.MinDuration > 0 {
		q.Set("minDuration", fmt.Sprintf("%d", query.MinDuration.Microseconds()))
	}
	if query.Limit > 0 {
		q.Set("limit", strconv.Itoa(query.Limit))
	}
	u.RawQuery = q.Encode()
	log.Debugf("Prepared Zipkin query: %v", u)
}

func makeRequest(client http.Client, endpoint string, body io.Reader) (response []byte, status int, err error) {
	response = nil
	status = 0

	req, err := http.NewRequest(http.MethodGet, endpoint, body)
	if err != nil {
		return
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	response, err = io.ReadAll(resp.Body)
	status = resp.StatusCode
	return
}

func convertZipkinToJaeger(zipkinResponse []byte) (*model.TracingResponse, error) {
	// Unmarshal Zipkin response to Zipkin model
	var zipkinSpans []model.SpanModel
	err := json.Unmarshal(zipkinResponse, &zipkinSpans)
	if err != nil {
		log.Errorf("Error unmarshalling Zipkin response to Zipkin model: %v", err)
		return nil, err
	}

	// Convert Zipkin model to Jaeger model
	var jaegerSpans []model.Span
	processes := make(map[model.ProcessID]model.Process)

	for _, zipkinSpan := range zipkinSpans {
		jaegerSpan := model.Span{
			TraceID:       model.TraceID(zipkinSpan.TraceID),
			SpanID:        model.SpanID(zipkinSpan.ID.String()),
			ParentSpanID:  model.SpanID(zipkinSpan.ParentID.String()),
			OperationName: zipkinSpan.Name,
			References:    convertZipkinReferences(zipkinSpan),
			StartTime:     uint64(zipkinSpan.Timestamp.UnixNano() / 1e3),
			Duration:      uint64(zipkinSpan.Duration.Nanoseconds() / 1e3),
			Tags:          convertZipkinTags(zipkinSpan.Tags),
			ProcessID:     model.ProcessID(zipkinSpan.LocalEndpoint.ServiceName),
			Process: &model.Process{
				ServiceName: zipkinSpan.LocalEndpoint.ServiceName,
				Tags:        convertZipkinTags(zipkinSpan.LocalEndpoint.Tags),
			},
		}

		jaegerSpans = append(jaegerSpans, jaegerSpan)
		processes[model.ProcessID(zipkinSpan.LocalEndpoint.ServiceName)] = *jaegerSpan.Process
	}

	// Construct Jaeger TracingResponse
	jaegerTrace := model.Trace{
		TraceID:   jaegerSpans[0].TraceID,
		Spans:     jaegerSpans,
		Processes: processes,
		Warnings:  nil, // Handle warnings if necessary
		Matched:   0,   // Handle matched if necessary
	}

	jaegerResponse := &model.TracingResponse{
		Data: []*model.Trace{&jaegerTrace},
	}

	return jaegerResponse, nil
}

func convertZipkinReferences(zipkinSpan model.SpanModel) []model.Reference {
	var references []model.Reference
	for _, zipkinRef := range zipkinSpan.Annotations {
		reference := model.Reference{
			RefType: convertZipkinReferenceType(zipkinRef.Value),
			TraceID: model.TraceID(zipkinSpan.TraceID),
			SpanID:  model.SpanID(zipkinRef.Endpoint.ServiceName),
		}
		references = append(references, reference)
	}
	return references
}

func convertZipkinReferenceType(value string) model.ReferenceType {
	if strings.ToUpper(value) == "CHILD_OF" {
		return model.ChildOf
	}
	return model.FollowsFrom
}

func convertZipkinTags(tags map[string]string) []model.KeyValue {
	var jaegerTags []model.KeyValue
	for key, value := range tags {
		jaegerTag := model.KeyValue{
			Key:   key,
			Type:  model.StringType,
			Value: value,
		}
		jaegerTags = append(jaegerTags, jaegerTag)
	}
	return jaegerTags
}
