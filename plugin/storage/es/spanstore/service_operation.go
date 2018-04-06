// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spanstore

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"
	"gopkg.in/olivere/elastic.v5"

	jModel "github.com/jaegertracing/jaeger/model/json"
	"github.com/jaegertracing/jaeger/pkg/cache"
	"github.com/jaegertracing/jaeger/pkg/es"
	storageMetrics "github.com/jaegertracing/jaeger/storage/spanstore/metrics"
	"strings"
	"encoding/json"
)

const (
	serviceName = "serviceName"

	operationsAggregation = "distinct_operations"
	servicesAggregation   = "distinct_services"
)

// ServiceOperationStorage stores service to operation pairs.
type ServiceOperationStorage struct {
	ctx          context.Context
	client       es.Client
	metrics      *storageMetrics.WriteMetrics
	logger       *zap.Logger
	serviceCache cache.Cache
}

// NewServiceOperationStorage returns a new ServiceOperationStorage.
func NewServiceOperationStorage(
	ctx context.Context,
	client es.Client,
	metricsFactory metrics.Factory,
	logger *zap.Logger,
	cacheTTL time.Duration,
) *ServiceOperationStorage {
	return &ServiceOperationStorage{
		ctx:    ctx,
		client: client,
		logger: logger,
		serviceCache: cache.NewLRUWithOptions(
			100000,
			&cache.Options{
				TTL: cacheTTL,
			},
		),
	}
}

func (s *ServiceOperationStorage) getClusterNameTag(jsonSpan *jModel.Span) string {

	for _, tag := range jsonSpan.Tags {
		if tag.Type == "string" && tag.Key == "cluster" {
			s.logger.Info("cluster name is ", zap.String(tag.Key, tag.Value.(string)))
			return tag.Value.(string)
		}
	}
	s.logger.Warn("cluster name is empty")
	return ""
}

func (s *ServiceOperationStorage) getNamespaceNameTag(jsonSpan *jModel.Span) string {

	for _, tag := range jsonSpan.Tags {
		if tag.Type == "string" && tag.Key == "namespace" {
			s.logger.Info("namespace name is ", zap.String(tag.Key, tag.Value.(string)))
			return tag.Value.(string)
		}
	}
	s.logger.Warn("Namespace name is empty")
	return ""
}

// Write saves a service to operation pair.
func (s *ServiceOperationStorage) Write(indexName string, jsonSpan *jModel.Span) {
	// Insert serviceName:operationName document
	service := Service{
		ServiceName:   jsonSpan.Process.ServiceName,
		OperationName: jsonSpan.OperationName,
		ClusterName:   s.getClusterNameTag(jsonSpan) + ":" + s.getNamespaceNameTag(jsonSpan),
		ProjectName:   s.getNamespaceNameTag(jsonSpan),
	}
	serviceID := fmt.Sprintf("%s|%s", service.ServiceName, service.OperationName)
	cacheKey := fmt.Sprintf("%s:%s:%s", indexName, serviceID, service.ClusterName)
	if !keyInCache(cacheKey, s.serviceCache) {
		s.client.Index().Index(indexName).Type(serviceType).Id(serviceID).BodyJson(service).Add()
		writeCache(cacheKey, s.serviceCache)
	} else {
		s.logger.Info("existed:", zap.String(serviceID, cacheKey))
	}
}

func (s *ServiceOperationStorage) getServices(indices []string) ([]string, error) {
	serviceAggregation := getServicesAggregation()

	searchService := s.client.Search(indices...).
		Type(serviceType).
		//Size(0). // set to 0 because we don't want actual documents.
		IgnoreUnavailable(true).
		Aggregation(servicesAggregation, serviceAggregation)

	searchResult, err := searchService.Do(s.ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Search service failed")
	}
	json, err := json.Marshal(searchResult)
	s.logger.Info("result : ", zap.String("json", string(json)))
	if searchResult.Aggregations == nil {
		return []string{}, nil
	}
	bucket, found := searchResult.Aggregations.Terms(servicesAggregation)
	if !found {
		return nil, errors.New("Could not find aggregation of " + servicesAggregation)
	}
	serviceNamesBucket := bucket.Buckets
	result, err := bucketToStringArray(serviceNamesBucket)
	s.logger.Info("result: ", zap.String("service", strings.Join(result, ":")))
	return result, err
}

func getServicesAggregation() elastic.Query {
	return elastic.NewTermsAggregation().
		Field(serviceName).
		Size(defaultDocCount) // Must set to some large number. ES deprecated size omission for aggregating all. https://github.com/elastic/elasticsearch/issues/18838
}

func bucketToStringArrayWithPrefix(prefix string, buckets []*elastic.AggregationBucketKeyItem) ([]string, error) {
	strings := make([]string, len(buckets))
	for i, keyitem := range buckets {
		str, ok := keyitem.Key.(string)
		if !ok {
			return nil, errors.New("Non-string key found in aggregation")
		}
		strings[i] = prefix + ":" +str
	}
	return strings, nil
}

func (s *ServiceOperationStorage) getClusterServices(indices []string) ([]string, error) {
	clusterAggregation := getClusterServicesAggregation()

	clustersAggregation := "distinct_clusters"
	searchService := s.client.Search(indices...).
		Type(serviceType).
		Size(0). // set to 0 because we don't want actual documents.
		IgnoreUnavailable(true).
		Aggregation(clustersAggregation, clusterAggregation)
	searchResult, err := searchService.Do(s.ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Search service failed")
	}
	json, err := json.Marshal(searchResult)
	s.logger.Info("result : ", zap.String("json", string(json)))
	if searchResult.Aggregations == nil {
		return []string{}, nil
	}
	bucket, found := searchResult.Aggregations.Terms(clustersAggregation)
	if !found {
		return nil, errors.New("Could not find aggregation of " + clustersAggregation)
	}
	clusterNamesBucket := bucket.Buckets
	s.logger.Info("", zap.String("cluster bucket :", string(len(clusterNamesBucket))))
	var result []string
	for _, bucket := range clusterNamesBucket {
		sub_bucket, found := bucket.Aggregations.Terms(servicesAggregation)
		s.logger.Info("", zap.String("service bucket :", string(len(sub_bucket.Buckets))))
		if found {
			services, err := bucketToStringArrayWithPrefix(bucket.Key.(string), sub_bucket.Buckets)
			if err == nil {
				result = append(result, services...)
			}
		}
	}

	s.logger.Info("result: ", zap.String("service", strings.Join(result, "-")))
	return result, err
}

func getClusterServicesAggregation() elastic.Query {
	return elastic.NewTermsAggregation().
		Field("clusterName").
		Size(defaultDocCount). // Must set to some large number. ES deprecated size omission for aggregating all. https://github.com/elastic/elasticsearch/issues/18838
		Include(".*admin.*|.*user.*").
		SubAggregation(servicesAggregation, elastic.NewTermsAggregation().Field(serviceName).Size(defaultDocCount))
}

func getClusterFromService(service string) ([]string, error) {
	clusterInfo := strings.Split(service,":")
	if len(clusterInfo) != 3 {
		return []string{}, errors.New("invalid service format")
	}
	return clusterInfo, nil
}

func getPrettyClusterFromService(clusterInfo []string) (string, string, error) {
	if len(clusterInfo) != 3 {
		return "", "", errors.New("invalid cluster format")
	}
	return (clusterInfo[0] + ":" + clusterInfo[1]), clusterInfo[2], nil
}

func (s *ServiceOperationStorage) getOperations(indices []string, service string) ([]string, error) {
	s.logger.Info("", zap.String("the name of service", service))

	clusterInfoList, err := getClusterFromService(service)
	if err != nil {
		return []string{}, err
	}

	clusterInfo, service, err := getPrettyClusterFromService(clusterInfoList)
	if err != nil {
		return []string{}, err
	}

	s.logger.Info("extracted info :", zap.String(clusterInfo, service))
	boolQuery := elastic.NewBoolQuery()
	boolQuery.Must(elastic.NewMatchQuery(serviceName, service), elastic.NewMatchQuery("clusterName", clusterInfo))
	//serviceQuery := elastic.NewTermQuery(serviceName, service)
	serviceFilter := getOperationsAggregation()

	searchService := s.client.Search(indices...).
		Type(serviceType).
		Size(0).
		//Query(serviceQuery).
		Query(boolQuery).
		IgnoreUnavailable(true).
		Aggregation(operationsAggregation, serviceFilter)

	searchResult, err := searchService.Do(s.ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Search service failed")
	}
	if searchResult.Aggregations == nil {
		return []string{}, nil
	}
	bucket, found := searchResult.Aggregations.Terms(operationsAggregation)
	if !found {
		return nil, errors.New("Could not find aggregation of " + operationsAggregation)
	}
	operationNamesBucket := bucket.Buckets
	return bucketToStringArray(operationNamesBucket)
}

func getOperationsAggregation() elastic.Query {
	return elastic.NewTermsAggregation().
		Field(operationNameField).
		Size(defaultDocCount) // Must set to some large number. ES deprecated size omission for aggregating all. https://github.com/elastic/elasticsearch/issues/18838
}

func (s *ServiceOperationStorage) getClusterName(indices []string, service string) ([]string, error) {
	serviceQuery := elastic.NewTermQuery(serviceName, service)
	serviceFilter := getClusterAggregation()

	searchService := s.client.Search(indices...).
		Type(serviceType).
		Size(0).
		Query(serviceQuery).
		IgnoreUnavailable(true).
		Aggregation(operationsAggregation, serviceFilter)

	searchResult, err := searchService.Do(s.ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Search service failed")
	}
	if searchResult.Aggregations == nil {
		return []string{}, nil
	}
	bucket, found := searchResult.Aggregations.Terms(operationsAggregation)
	if !found {
		return nil, errors.New("Could not find aggregation of " + operationsAggregation)
	}
	operationNamesBucket := bucket.Buckets
	return bucketToStringArray(operationNamesBucket)
}

func getClusterAggregation() elastic.Query {
	return elastic.NewTermsAggregation().
		Field(operationNameField).
		Size(defaultDocCount) // Must set to some large number. ES deprecated size omission for aggregating all. https://github.com/elastic/elasticsearch/issues/18838
}
