import sys
import PIL.Image
import PIL.ImageChops

if sys.version_info.major < 3:
	from StringIO import StringIO
else:
	from io import StringIO 
    
import json

import rostful.message_conversion as msgconv
import rostful.server


class Transform(object):
    def get(self, start_response, content, **kwargs):
        raise NotImplementedError

    def post(self, start_response, content, **kwargs):
        raise NotImplementedError

    def valid(self):
        return True


class GetTransform(Transform):
    def post(self, start_response, content, **kwargs):
        return server.response_405(start_response, "POST Operations not allowed with this transform!")


class JsonTransform(GetTransform):
    def get(self, start_response, content, **kwargs):
        if content:
            out = msgconv.extract_values(content) if content is not None else None
            out = json.dumps(out)
        else:
            out = "Topic found, but no content is available."

        return server.response_200(start_response, str(out), content_type='application/json')


class TextTransform(GetTransform):
    def get(self, start_response, content, **kwargs):
        if content:
            out = str(content)
        else:
            out = "Topic found, but no content is available."

        return server.response_200(start_response, out, content_type='application/json')


class ImageTransform(GetTransform):
    def get_image_type(self):
        return 'jpeg'

    def get_mime_type(self):
        return 'image/%s' % self.get_image_type()

    def can_use_palette(self):
        return False

    def get(self, start_response, content, maxsize=0, crop=False, **kwargs):
        maxsize = int(maxsize)
        crop = bool(crop)
        if content.__class__.__name__ == "OccupancyGrid":
            #im = PIL.Image.frombytes('P', (content.info.width, content.info.height), bytes(content.data))

            from numpy import array
            im_bytes = array(content.data)
            im_bytes.shape = (content.info.width, content.info.height)
            im_bytes = im_bytes.astype('u1')


            im = PIL.Image.fromarray(im_bytes, 'P')
            #generate the color palette
            palette = []
            for i in range(0, 256):
                if i <= 100:
                    palette.extend([(100-i) * 255/100] * 3)
                else:
                    palette.extend([170, 170, 255])
            im.putpalette(palette)
            if crop:
                bg = PIL.Image.new(im.mode, im.size, im.getpixel((0,0)))
                diff = PIL.ImageChops.difference(im, bg)
                bbox = diff.getbbox()
                if bbox:
                     im = im.crop(bbox)
                     print("Cropped")
                else:
                     im = bg
            if maxsize > 0 and maxsize < max(im.size[0], im.size[1]):
                im.thumbnail([maxsize]*2)
            if not self.can_use_palette():
                im = im.convert()
            output = StringIO.StringIO()
            im.save(output, format=self.get_image_type())
            return server.response_200(start_response,output.getvalue(), content_type=self.get_mime_type())
        else:
            return server.response_500(start_response,"No image converter for this topic type.")


class GifTransform(ImageTransform):
    def get_image_type(self):
        return 'gif'

    def can_use_palette(self):
        return True


class PngTransform(ImageTransform):
    def get_image_type(self):
        return 'png'

    def can_use_palette(self):
        return True


class BadTransform(Transform):
    def get(self, start_response, content, **kwargs):
        return server.response_405(start_response, "Invalid Transform!")

    def post(self, start_response, content, **kwargs):
        return server.response_405(start_response, "Invalid Transform!")


class NullTransform(BadTransform):
    def valid(self):
        return False

mappings = {'json': JsonTransform(),
            'txt':  TextTransform(),
            'jpeg': ImageTransform(),
            'jpg':  ImageTransform(),
            'gif':  GifTransform(),
            'png':  PngTransform()}
