//
// MainPage.xaml.cpp
// Implementation of the MainPage class.
//

#include "pch.h"
#include "MainPage.xaml.h"
#include "realm.hpp"
using namespace App1;
using namespace realm;

using namespace Platform;
using namespace Windows::Foundation;
using namespace Windows::Foundation::Collections;
using namespace Windows::UI::Xaml;
using namespace Windows::UI::Xaml::Controls;
using namespace Windows::UI::Xaml::Controls::Primitives;
using namespace Windows::UI::Xaml::Data;
using namespace Windows::UI::Xaml::Input;
using namespace Windows::UI::Xaml::Media;
using namespace Windows::UI::Xaml::Navigation;

// For ARM, only Release mode is currently configured. Build Core in ARM Release mode too.

Table* tbl = nullptr;

MainPage::MainPage()
{
	InitializeComponent();
}

void App1::MainPage::Button_Click(Platform::Object^ sender, Windows::UI::Xaml::RoutedEventArgs^ e)
{
    if(!tbl) {
        tbl = new Table();
        tbl->add_column(type_String, "strings");
        tbl->add_empty_row();
    }
    
    Platform::String^ ps = MyTextBox->Text;
    std::wstring ws = ps->Data();
    StringData sd((char*)ws.data(), ws.size()*sizeof(wchar_t));
    tbl->set_string(0, 0, sd);

}


void App1::MainPage::TextBlock_SelectionChanged(Platform::Object^ sender, Windows::UI::Xaml::RoutedEventArgs^ e)
{
    
}


void App1::MainPage::Button_Click_1(Platform::Object^ sender, Windows::UI::Xaml::RoutedEventArgs^ e)
{
    StringData sd = tbl->get_string(0, 0);
    std::wstring s = std::wstring((wchar_t*)sd.data());
    Platform::String^ ps = ref new Platform::String(s.c_str());
    MyTextBlock->Text = ps;
}
