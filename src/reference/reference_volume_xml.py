VOLUME_XML="""
<disk type='network' device='disk'>
<driver name="qemu" type="raw"/>
<source protocol='rbd' name='%s/%s'>
<host name='%s' port='6789'/>
</source>
<target dev='%s' bus='virtio'/>
</disk>"""





