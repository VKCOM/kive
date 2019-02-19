package worker

import (
	"reflect"
	"testing"
)

func Test_composeVirtual(t *testing.T) {
	result, _ := composeVirtual(240, []int{240, 480, 720, 1080}, []int{240})
	if reflect.DeepEqual([]int{480, 720, 1080}, result) == false {
		t.Errorf("%+v", result)
	}

	result, _ = composeVirtual(240, []int{240, 480, 720, 1080}, []int{240, 720})
	if reflect.DeepEqual([]int{480}, result) == false {
		t.Errorf("%+v", result)
	}

	result, _ = composeVirtual(240, []int{240, 480, 720, 1080}, []int{240, 480})
	if reflect.DeepEqual([]int{}, result) == false {
		t.Errorf("%+v", result)
	}

	result, _ = composeVirtual(1080, []int{240, 480, 720, 1080}, []int{240, 480, 720, 1080})
	if reflect.DeepEqual([]int{}, result) == false {
		t.Errorf("%+v", result)
	}

	result, _ = composeVirtual(480, []int{240, 480, 720, 1080}, []int{240, 480, 1080})
	if reflect.DeepEqual([]int{720}, result) == false {
		t.Errorf("%+v", result)
	}

}
