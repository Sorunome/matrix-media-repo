package download_controller

import (
	"context"
	"database/sql"
	"os"
	"time"
	"strings"

	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/turt2live/matrix-media-repo/common"
	"github.com/turt2live/matrix-media-repo/common/config"
	"github.com/turt2live/matrix-media-repo/internal_cache"
	"github.com/turt2live/matrix-media-repo/storage"
	"github.com/turt2live/matrix-media-repo/types"
	"github.com/turt2live/matrix-media-repo/util"
)

var localCache = cache.New(30*time.Second, 60*time.Second)

func GetMedia(origin string, mediaId string, downloadRemote bool, blockForMedia bool, ctx context.Context, log *logrus.Entry) (*types.MinimalMedia, error) {
	var media *types.Media
	var minMedia *types.MinimalMedia
	var err error
	if blockForMedia {
		media, err = FindMediaRecord(origin, mediaId, downloadRemote, ctx, log)
		if media != nil {
			minMedia = &types.MinimalMedia{
				Origin:      media.Origin,
				MediaId:     media.MediaId,
				ContentType: media.ContentType,
				UploadName:  media.UploadName,
				SizeBytes:   media.SizeBytes,
				Stream:      nil, // we'll populate this later if we need to
				KnownMedia:  media,
			}
		}
	} else {
		minMedia, err = FindMinimalMediaRecord(origin, mediaId, downloadRemote, ctx, log)
		if minMedia != nil {
			media = minMedia.KnownMedia
		}
	}
	if err != nil {
		return nil, err
	}
	if minMedia == nil {
		log.Warn("Unexpected error while fetching media: no minimal media record")
		return nil, common.ErrMediaNotFound
	}
	if media == nil && blockForMedia {
		log.Warn("Unexpected error while fetching media: no regular media record (block for media in place)")
		return nil, common.ErrMediaNotFound
	}

	// if we have a known media record, we might as well set it
	// if we don't, this won't do anything different
	minMedia.KnownMedia = media

	if media != nil {
		if media.Quarantined {
			log.Warn("Quarantined media accessed")
			return nil, common.ErrMediaQuarantined
		}

		err = storage.GetDatabase().GetMetadataStore(ctx, log).UpsertLastAccess(media.Sha256Hash, util.NowMillis())
		if err != nil {
			logrus.Warn("Failed to upsert the last access time: ", err)
		}

		localCache.Set(origin+"/"+mediaId, media, cache.DefaultExpiration)
		internal_cache.Get().IncrementDownloads(media.Sha256Hash)

		cached, err := internal_cache.Get().GetMedia(media, log)
		if err != nil {
			return nil, err
		}
		if cached != nil && cached.Contents != nil {
			minMedia.Stream = util.BufferToStream(cached.Contents)
			return minMedia, nil
		}
	}

	if minMedia.Stream != nil {
		log.Info("Returning minimal media record with a viable stream")
		return minMedia, nil
	}

	log.Info("Reading media from disk")
	filePath, err := storage.ResolveMediaLocation(ctx, log, media.DatastoreId, media.Location)
	if err != nil {
		return nil, err
	}
	stream, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	minMedia.Stream = stream
	return minMedia, nil
}

func FindMinimalMediaRecord(origin string, mediaId string, downloadRemote bool, ctx context.Context, log *logrus.Entry) (*types.MinimalMedia, error) {
	db := storage.GetDatabase().GetMediaStore(ctx, log)

	var media *types.Media
	item, found := localCache.Get(origin + "/" + mediaId)
	if found {
		media = item.(*types.Media)
	} else {
		staticMedia, err := searchStaticMinimalMedia(origin, mediaId, log)
		if err == nil {
			return staticMedia, nil
		}
		log.Info("Getting media record from database")
		dbMedia, err := db.Get(origin, mediaId)
		if err != nil {
			if err == sql.ErrNoRows {
				if util.IsServerOurs(origin) {
					log.Warn("Media not found")
					return nil, common.ErrMediaNotFound
				}
			}

			if !downloadRemote {
				log.Warn("Remote media not being downloaded")
				return nil, common.ErrMediaNotFound
			}

			result := <-getResourceHandler().DownloadRemoteMedia(origin, mediaId, false)
			if result.err != nil {
				return nil, result.err
			}
			return &types.MinimalMedia{
				Origin:      origin,
				MediaId:     mediaId,
				ContentType: result.contentType,
				UploadName:  result.filename,
				SizeBytes:   -1, // unknown
				Stream:      result.stream,
				KnownMedia:  nil, // unknown
			}, nil
		} else {
			media = dbMedia
		}
	}

	if media == nil {
		log.Warn("Despite all efforts, a media record could not be found")
		return nil, common.ErrMediaNotFound
	}

	filePath, err := storage.ResolveMediaLocation(ctx, log, media.DatastoreId, media.Location)
	if err != nil {
		return nil, err
	}

	stream, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	return &types.MinimalMedia{
		Origin:      media.Origin,
		MediaId:     media.MediaId,
		ContentType: media.ContentType,
		UploadName:  media.UploadName,
		SizeBytes:   media.SizeBytes,
		Stream:      stream,
		KnownMedia:  media,
	}, nil
}

func FindMediaRecord(origin string, mediaId string, downloadRemote bool, ctx context.Context, log *logrus.Entry) (*types.Media, error) {
	db := storage.GetDatabase().GetMediaStore(ctx, log)

	var media *types.Media
	item, found := localCache.Get(origin + "/" + mediaId)
	if found {
		media = item.(*types.Media)
	} else {
		staticMedia, err := searchStaticMedia(origin, mediaId, log)
		if err == nil {
			return staticMedia, nil
		}
		log.Info("Getting media record from database")
		dbMedia, err := db.Get(origin, mediaId)
		if err != nil {
			if err == sql.ErrNoRows {
				if util.IsServerOurs(origin) {
					log.Warn("Media not found")
					return nil, common.ErrMediaNotFound
				}
			}

			if !downloadRemote {
				log.Warn("Remote media not being downloaded")
				return nil, common.ErrMediaNotFound
			}

			result := <-getResourceHandler().DownloadRemoteMedia(origin, mediaId, true)
			if result.err != nil {
				return nil, result.err
			}
			media = result.media
		} else {
			media = dbMedia
		}
	}

	if media == nil {
		log.Warn("Despite all efforts, a media record could not be found")
		return nil, common.ErrMediaNotFound
	}

	return media, nil
}

type staticMediaReply struct {
	Path string
	Filename string
	ContentType string
}

func lookupStaticMedia(origin string, mediaId string, log *logrus.Entry) (*staticMediaReply, error) {
	log.Info("Searching static content")
	for _, v := range config.Get().StaticContents {
		// first check if it is for our server
		// and check if the MXC url starts with our prefix
		if v.Server != origin || !strings.HasPrefix(mediaId, v.MxcPrefix) {
			continue
		}

		// okay, this is something for us to handle!
		sanitizedMediaId := mediaId[len(v.MxcPrefix):]
		// sanity check
		if strings.ContainsAny(sanitizedMediaId, "/") {
			return nil, common.ErrMediaNotFound
		}

		path := v.Directory + "/"
		filename := ""
		contentType := ""
		if len(v.TryFiles) == 0 {
			filename = sanitizedMediaId
			_, err := os.Stat(path + filename)
			if err != nil {
				continue
			}
		} else {
			for _, t := range v.TryFiles {
				filename = t.Prefix + sanitizedMediaId + t.Suffix
				_, err := os.Stat(path + filename)
				if err != nil {
					continue
				}
				// we have a match!
				contentType = t.ContentType
				break
			}
		}

		path += filename

		if contentType == "" {
			c, err := storage.GetFileContentType(path)
			if err != nil {
				return nil, common.ErrMediaNotFound
			}
			contentType = c
		}
		log.Info("matched static content")
		return &staticMediaReply{
			Path: path,
			Filename: filename,
			ContentType: contentType,
		}, nil
	}
	return nil, common.ErrMediaNotFound
}

func searchStaticMinimalMedia(origin string, mediaId string, log *logrus.Entry) (*types.MinimalMedia, error) {
	res, err := lookupStaticMedia(origin, mediaId, log)
	if err != nil {
		return nil, common.ErrMediaNotFound
	}

	log.Info("found valid static file")
	return &types.MinimalMedia{
		Origin:      origin,
		MediaId:     mediaId,
		ContentType: res.ContentType,
		UploadName:  res.Filename,
		SizeBytes:   -1,
		Stream:      nil,
		KnownMedia:  nil,
	}, nil
}

func searchStaticMedia(origin string, mediaId string, log *logrus.Entry) (*types.Media, error) {
	res, err := lookupStaticMedia(origin, mediaId, log)
	if err != nil {
		return nil, common.ErrMediaNotFound
	}

	file, err := os.Open(res.Path)
	if err != nil {
		return nil, common.ErrMediaNotFound
	}
	fi, err := file.Stat()
	if err != nil {
		return nil, common.ErrMediaNotFound
	}
	defer file.Close()

	hash, err := storage.GetFileHash(res.Path)
	if err != nil {
		return nil, common.ErrMediaNotFound
	}

	log.Info("found valid static file")
	return &types.Media{
		Origin: origin,
		MediaId: mediaId,
		UploadName: res.Filename,
		ContentType: res.ContentType,
		UserId: "",
		Sha256Hash: hash,
		SizeBytes: fi.Size(),
		Location: res.Path,
		CreationTs: fi.ModTime().Unix(),
		Quarantined: false,
	}, nil
}
