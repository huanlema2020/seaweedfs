package filesys

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
)

func (wfs *WFS) saveDataAsChunk(dir string) filer.SaveDataAsChunkFunctionType {

	return func(reader io.Reader, filename string, offset int64) (chunk *filer_pb.FileChunk, collection, replication string, err error) {
		var fileId, host string
		var auth security.EncodedJwt

		resp := wfs.fetchFileId(dir)
		fileId, auth = resp.FileId, security.EncodedJwt(resp.Auth)
		loc := &filer_pb.Location{
			Url:       resp.Url,
			PublicUrl: resp.PublicUrl,
		}
		host = wfs.AdjustedUrl(loc)
		collection, replication = resp.Collection, resp.Replication

		fileUrl := fmt.Sprintf("http://%s/%s", host, fileId)
		uploadResult, err, data := operation.Upload(fileUrl, filename, wfs.option.Cipher, reader, false, "", nil, auth)
		if err != nil {
			glog.V(0).Infof("upload data %v to %s: %v", filename, fileUrl, err)
			return nil, "", "", fmt.Errorf("upload data: %v", err)
		}
		if uploadResult.Error != "" {
			glog.V(0).Infof("upload failure %v to %s: %v", filename, fileUrl, err)
			return nil, "", "", fmt.Errorf("upload result: %v", uploadResult.Error)
		}

		wfs.chunkCache.SetChunk(fileId, data)

		chunk = uploadResult.ToPbFileChunk(fileId, offset)
		return chunk, collection, replication, nil
	}
}

func (wfs *WFS) prefetchFileId() {
	wfs.prefetchedCollection = wfs.option.Collection
	for {
		if resp, err := wfs.doAssignFileId(wfs.prefetchedCollection); err == nil {
			wfs.fileIdChan <- resp
			// glog.V(0).Infof("fetched file id %s collection %s", resp.resp.FileId, resp.resp.Collection)
		} else {
			if err != nil {
				glog.Errorf("assign file id: %v", err)
			}
			time.Sleep(123 * time.Millisecond)
		}
	}
}

func (wfs *WFS) fetchFileId(dir string) *filer_pb.AssignVolumeResponse {
	requiredCollection := wfs.detectCollection(dir)
	wfs.prefetchedCollection = requiredCollection
	for r := range wfs.fileIdChan {
		if r.resp.Collection == requiredCollection {
			if r.resp.Auth != "" && r.ts.Add(time.Second).Before(time.Now()) {
				// glog.V(0).Infof("skipping old file id %s collection %s", r.resp.FileId, r.resp.Collection)
				continue
			}
			// glog.V(0).Infof("received file id %s collection %s", r.resp.FileId, r.resp.Collection)
			return r.resp
		}
		// glog.V(0).Infof("skipping file id %s collection %s", r.resp.FileId, r.resp.Collection)
	}
	return nil
}

func (wfs *WFS) detectCollection(dir string) (collection string) {
	collection = wfs.option.Collection
	if strings.HasPrefix(dir, wfs.option.FilerBucketsPath+"/") {
		bucketAndObjectKey := dir[len(wfs.option.FilerBucketsPath)+1:]
		t := strings.Index(bucketAndObjectKey, "/")
		if t < 0 {
			collection = bucketAndObjectKey
		}
		if t > 0 {
			collection = bucketAndObjectKey[:t]
		}
	}
	return
}

type PreAssignFileIdResult struct {
	resp *filer_pb.AssignVolumeResponse
	ts   time.Time
}

func (wfs *WFS) doAssignFileId(collection string) (result *PreAssignFileIdResult, err error) {

	var resp *filer_pb.AssignVolumeResponse
	err = wfs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.AssignVolumeRequest{
			Count:       1,
			Replication: wfs.option.Replication,
			Collection:  collection,
			TtlSec:      wfs.option.TtlSec,
			DataCenter:  wfs.option.DataCenter,
		}

		resp, err = client.AssignVolume(context.Background(), request)
		if err != nil {
			glog.V(0).Infof("assign volume failure %v: %v", request, err)
			return err
		}
		if resp.Error != "" {
			return fmt.Errorf("assign volume failure %v: %v", request, resp.Error)
		}

		return nil
	})

	return &PreAssignFileIdResult{
		resp: resp,
		ts:   time.Now(),
	}, err

}
