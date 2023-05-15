package utils

import (
	"github.com/pkg/errors"
	"log"
	"sync/atomic"
	"unsafe"
)

const (
	offsetSize  = int(unsafe.Sizeof(uint32(0)))
	nodeAlign   = int(unsafe.Sizeof(uint64(0))) - 1
	MaxNodeSize = int(unsafe.Sizeof(node{}))
)

type Arena struct {
	n          uint32
	shouldGrow bool
	buf        []byte
}

func newArena(n int64) *Arena {
	return &Arena{
		n:   1,
		buf: make([]byte, n),
	}
}

func (s *Arena) putKey(key []byte) uint32 {
	keySz := uint32(len(key))
	offset := s.allocate(keySz)
	buf := s.buf[offset : offset+keySz]
	//比较key的长度和写入的是否一致
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

func (s *Arena) putVal(v ValueStruct) uint32 {
	l := uint32(v.EncodedSize())
	offset := s.allocate(l)
	v.EncodeValue(s.buf[offset:])
	return offset
}

func (s *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&s.buf[offset]))
}

func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

func (s *Arena) getVal(offset uint32, size uint32) (ret ValueStruct) {
	ret.DecodeValue(s.buf[offset : offset+size])
	return
}

func (s *Arena) putNode(height int) uint32 {
	unusedSize := (maxHeight - height) * offsetSize

	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	n := s.allocate(l)

	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}

//fixme-:
func (s *Arena) allocate(sz uint32) uint32 {
	// 这里使用原子操作, 在已经占用的内存的数值上 + 要分配大小
	offset := atomic.AddUint32(&s.n, sz)
	if !s.shouldGrow {
		AssertTrue(int(offset) <= len(s.buf))
		return offset - sz
	}

	// 要分配的内存空间, 已经不足以放下下一个新的节点
	if int(offset) > len(s.buf)-MaxNodeSize {
		//把arena的空间double一下
		growBy := uint32(len(s.buf))
		if growBy > 1<<30 {
			growBy = 1 << 30
		}
		if growBy < sz {
			growBy = sz
		}
		newBuf := make([]byte, len(s.buf)+int(growBy))
		//这里的操作是RCU, 全量Copy到新的Buf中,然后设置为新的Arena内存值
		AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		//这里进行新的赋值
		s.buf = newBuf
		// fmt.Print(len(s.buf), " ")
	}
	return offset - sz
}

func (s *Arena) size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

func (s *Arena) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0 //返回空指针
	}
	//implement me here！！！
	//获取某个节点,在 arena 当中的偏移量
	//unsafe.Pointer等价于void*,uintptr可以专门把void*的对于地址转化为数值型变量
	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
